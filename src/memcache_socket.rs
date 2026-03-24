use std::os::fd::RawFd;

use pyo3::BoundObject;
use pyo3::exceptions::{PyConnectionError, PyTimeoutError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::constants::*;
use crate::impl_parse_header::{ParsedHeader, impl_parse_header};
use crate::response_types::*;

const DEFAULT_BUFFER_SIZE: usize = 4096;

/// Convert a Rust pyclass into a `Py<PyAny>` for returning from methods
/// that return different Python types (union return).
fn into_py<'py, T: IntoPyObject<'py>>(py: Python<'py>, obj: T) -> PyResult<Py<PyAny>>
where
    T::Error: Into<PyErr>,
{
    Ok(obj
        .into_pyobject(py)
        .map_err(Into::into)?
        .into_any()
        .unbind())
}

fn socket_err(msg: &str) -> PyErr {
    PyConnectionError::new_err(msg.to_string())
}

fn socket_err_io(msg: &str, source: std::io::Error) -> PyErr {
    if source.kind() == std::io::ErrorKind::TimedOut {
        PyTimeoutError::new_err("timed out")
    } else {
        PyConnectionError::new_err(format!("{msg}: {source}"))
    }
}

/// Read the timeout from a Python socket object and convert to poll() milliseconds.
/// Returns -1 for blocking sockets (timeout is None), or a positive ms value.
fn get_timeout_ms(conn: &Bound<'_, PyAny>) -> PyResult<libc::c_int> {
    let timeout_obj = conn.call_method0("gettimeout")?;
    if timeout_obj.is_none() {
        Ok(-1)
    } else {
        let seconds: f64 = timeout_obj.extract()?;
        // Convert seconds to milliseconds, clamping to valid range.
        // A timeout of 0 means non-blocking (don't wait at all).
        let ms = (seconds * 1000.0).ceil() as i64;
        Ok(ms.clamp(0, libc::c_int::MAX as i64) as libc::c_int)
    }
}

/// Wait for the fd to become ready for reading/writing using poll().
/// This handles non-blocking sockets set up via Python's settimeout().
/// `timeout_ms` is the poll timeout: -1 for infinite (blocking sockets),
/// or a positive value in milliseconds (from Python's settimeout()).
#[inline]
fn poll_fd(
    fd: RawFd,
    events: libc::c_short,
    timeout_ms: libc::c_int,
) -> Result<(), std::io::Error> {
    let mut pfd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    // SAFETY: pfd is a valid pollfd struct on the stack, nfds=1
    let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::Interrupted {
            return poll_fd(fd, events, timeout_ms); // signal interrupted us, retry
        }
        Err(err)
    } else if ret == 0 {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        ))
    } else if pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "poll error on socket",
        ))
    } else {
        Ok(())
    }
}

/// Send all bytes through the fd, handling partial writes and EAGAIN.
#[inline]
fn send_all(fd: RawFd, data: &[u8], timeout_ms: libc::c_int) -> Result<(), std::io::Error> {
    let mut sent = 0;
    while sent < data.len() {
        // SAFETY: data[sent..] is a valid byte slice, fd is a valid socket
        let n = unsafe {
            libc::send(
                fd,
                data[sent..].as_ptr() as *const libc::c_void,
                data.len() - sent,
                0,
            )
        };
        if n > 0 {
            sent += n as usize;
        } else if n < 0 {
            let err = std::io::Error::last_os_error();
            match err.kind() {
                std::io::ErrorKind::WouldBlock => poll_fd(fd, libc::POLLOUT, timeout_ms)?,
                std::io::ErrorKind::Interrupted => continue,
                _ => return Err(err),
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "send returned 0",
            ));
        }
    }
    Ok(())
}

/// Send data + NOOP command in a single write when possible.
/// Uses writev() to avoid concatenation on the happy path, falls back
/// to send_all() for partial writes and EAGAIN.
#[inline]
fn send_all_with_noop(
    fd: RawFd,
    data: &[u8],
    timeout_ms: libc::c_int,
) -> Result<(), std::io::Error> {
    let iov = [
        libc::iovec {
            iov_base: data.as_ptr() as *mut libc::c_void,
            iov_len: data.len(),
        },
        libc::iovec {
            iov_base: NOOP_CMD.as_ptr() as *mut libc::c_void,
            iov_len: NOOP_CMD.len(),
        },
    ];
    // SAFETY: iov array has 2 valid entries pointing to data and NOOP_CMD
    let n = unsafe { libc::writev(fd, iov.as_ptr(), 2) };
    let total_len = data.len() + NOOP_CMD.len();
    let written = if n >= 0 {
        n as usize
    } else {
        let err = std::io::Error::last_os_error();
        match err.kind() {
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => 0,
            _ => return Err(err),
        }
    };
    if written < total_len {
        let combined: Vec<u8> = [data, NOOP_CMD].concat();
        send_all(fd, &combined[written..], timeout_ms)?;
    }
    Ok(())
}

/// Recv into buffer slice, returns bytes read. Handles EAGAIN by polling.
fn recv_into(fd: RawFd, buf: &mut [u8], timeout_ms: libc::c_int) -> Result<usize, std::io::Error> {
    loop {
        // SAFETY: buf is a valid mutable byte slice, fd is a valid socket
        let n = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
        if n > 0 {
            return Ok(n as usize);
        } else if n == 0 {
            return Ok(0);
        } else {
            let err = std::io::Error::last_os_error();
            match err.kind() {
                std::io::ErrorKind::WouldBlock => poll_fd(fd, libc::POLLIN, timeout_ms)?,
                std::io::ErrorKind::Interrupted => continue,
                _ => return Err(err),
            }
        }
    }
}

/// Recv filling the buffer completely. Handles EAGAIN by polling.
fn recv_fill(fd: RawFd, buf: &mut [u8], timeout_ms: libc::c_int) -> Result<usize, std::io::Error> {
    let mut total = 0;
    let size = buf.len();
    while total < size {
        // SAFETY: buf[total..] is a valid mutable byte slice, fd is a valid socket
        let n = unsafe {
            libc::recv(
                fd,
                buf[total..].as_mut_ptr() as *mut libc::c_void,
                size - total,
                libc::MSG_WAITALL,
            )
        };
        if n > 0 {
            total += n as usize;
        } else if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed during recv_fill",
            ));
        } else {
            let err = std::io::Error::last_os_error();
            match err.kind() {
                std::io::ErrorKind::WouldBlock => poll_fd(fd, libc::POLLIN, timeout_ms)?,
                std::io::ErrorKind::Interrupted => continue,
                _ => return Err(err),
            }
        }
    }
    Ok(total)
}

/// Where the value data ended up after recv.
enum ValueData {
    /// Value is in io.buf[pos..pos+size], ENDL validated. Caller advances pos.
    InBuffer,
    /// Value was too large for the buffer, stored in this Vec.
    Allocated(Vec<u8>),
}

/// Inner I/O state — no Python objects, so it is Send/Ungil.
/// This allows releasing the GIL during socket I/O via py.detach().
struct SocketIO {
    fd: RawFd,
    buf: Vec<u8>,
    buffer_size: usize,
    reset_buffer_size: usize,
    pos: usize,
    read: usize,
    noop_expected: u32,
    /// poll() timeout in milliseconds. -1 for blocking sockets (no timeout),
    /// positive value from Python's socket.settimeout().
    timeout_ms: libc::c_int,
}

impl SocketIO {
    fn recv_into_buffer(&mut self) -> Result<usize, std::io::Error> {
        let n = recv_into(self.fd, &mut self.buf[self.read..], self.timeout_ms)?;
        if n > 0 {
            self.read += n;
        }
        Ok(n)
    }

    fn reset_buffer(&mut self) {
        let remaining = self.read - self.pos;
        if remaining > 0 {
            self.buf.copy_within(self.pos..self.read, 0);
        }
        self.pos = 0;
        self.read = remaining;
    }

    fn get_single_header(&mut self) -> Result<ParsedHeader, std::io::Error> {
        if self.pos >= self.read {
            self.read = 0;
            self.pos = 0;
        } else if self.pos > self.reset_buffer_size {
            self.reset_buffer();
        }

        loop {
            if self.read != self.pos
                && let Some(header) = impl_parse_header(&self.buf, self.pos, self.read)
            {
                self.pos = header.end_pos;
                return Ok(header);
            }
            let n = self.recv_into_buffer()?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "Bad response. Socket might have closed unexpectedly",
                ));
            }
        }
    }

    fn read_until_noop_header(&mut self) -> Result<(), std::io::Error> {
        while self.noop_expected > 0 {
            let header = self.get_single_header()?;
            if header.response_type == Some(RESPONSE_NOOP) {
                self.noop_expected -= 1;
            }
        }
        Ok(())
    }

    fn get_header(&mut self) -> Result<ParsedHeader, std::io::Error> {
        if self.noop_expected > 0 {
            self.read_until_noop_header()?;
        }
        self.get_single_header()
    }

    fn sendall_impl(&self, data: &[u8], with_noop: bool) -> Result<(), std::io::Error> {
        if with_noop {
            send_all_with_noop(self.fd, data, self.timeout_ms)
        } else {
            send_all(self.fd, data, self.timeout_ms)
        }
    }

    /// Ensure value data is available for reading.
    ///
    /// For the common case (value fits in buffer), returns InBuffer —
    /// the data is at buf[pos..pos+size] with ENDL validated.
    /// The caller creates PyBytes directly from the buffer slice (zero-copy to Rust).
    ///
    /// For large values exceeding the buffer, returns Allocated with the data.
    fn ensure_value(&mut self, size: usize) -> Result<ValueData, std::io::Error> {
        let message_size = size + ENDL_LEN;

        // Try to fill buffer with enough data
        let mut data_in_buf = self.read - self.pos;
        while data_in_buf < message_size && self.read < self.buffer_size {
            let n = self.recv_into_buffer()?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "Connection closed while reading value",
                ));
            }
            data_in_buf = self.read - self.pos;
        }

        if data_in_buf >= message_size {
            // Value + ENDL fully in buffer — validate ENDL in place
            let data_end = self.pos + size;
            if self.buf[data_end] != b'\r' || self.buf[data_end + 1] != b'\n' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Value not terminated with \\r\\n",
                ));
            }
            // Don't advance pos yet — caller reads buf[pos..pos+size] then advances
            Ok(ValueData::InBuffer)
        } else if data_in_buf >= size {
            // Value in buffer but ENDL partially/not in buffer.
            // We still return InBuffer, but need to read+validate the ENDL.
            let data_end = self.pos + size;
            let endl_in_buf = data_in_buf - size;
            let mut endl_buf = [0u8; ENDL_LEN];
            if endl_in_buf > 0 {
                endl_buf[..endl_in_buf].copy_from_slice(&self.buf[data_end..self.read]);
            }
            if endl_in_buf < ENDL_LEN {
                recv_fill(self.fd, &mut endl_buf[endl_in_buf..], self.timeout_ms)?;
            }
            if endl_buf != *ENDL {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Value not terminated with \\r\\n",
                ));
            }
            // Discard partial ENDL bytes from the buffer's tracked range.
            // The caller will advance pos by size + ENDL_LEN, which will
            // overshoot read — get_single_header handles this via pos >= read.
            self.read = data_end;
            Ok(ValueData::InBuffer)
        } else {
            // Value doesn't fit in buffer — allocate and read into temp buffer
            let mut message = vec![0u8; message_size];
            message[..data_in_buf].copy_from_slice(&self.buf[self.pos..self.read]);
            recv_fill(self.fd, &mut message[data_in_buf..], self.timeout_ms)?;

            if message[size] != b'\r' || message[size + 1] != b'\n' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Value not terminated with \\r\\n",
                ));
            }

            self.pos = self.read; // Buffer fully consumed
            message.truncate(size);
            Ok(ValueData::Allocated(message))
        }
    }
}

#[pyclass]
pub struct MemcacheSocket {
    io: SocketIO,
    /// Hold a reference to the Python socket to prevent GC.
    _conn: Py<PyAny>,
    version: u8,
}

#[pymethods]
impl MemcacheSocket {
    #[new]
    #[pyo3(signature = (conn, buffer_size=DEFAULT_BUFFER_SIZE, version=SERVER_VERSION_STABLE))]
    pub fn new(conn: &Bound<'_, PyAny>, buffer_size: usize, version: u8) -> PyResult<Self> {
        let fd: RawFd = conn.call_method0("fileno")?.extract()?;
        let timeout_ms = get_timeout_ms(conn)?;

        // Set SO_RCVBUF — failure is non-fatal (kernel may reject the size)
        let recv_buf_size: libc::c_int = buffer_size as libc::c_int;
        // SAFETY: fd is a valid socket, recv_buf_size is a valid c_int on the stack
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &recv_buf_size as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if ret != 0 {
            // Non-fatal: log would be ideal but we don't have a logger here.
            // The socket will still work with the kernel default buffer size.
            let _ = ret;
        }

        Ok(MemcacheSocket {
            io: SocketIO {
                fd,
                buf: vec![0u8; buffer_size],
                buffer_size,
                reset_buffer_size: buffer_size * 3 / 4,
                pos: 0,
                read: 0,
                noop_expected: 0,
                timeout_ms,
            },
            _conn: conn.clone().unbind(),
            version,
        })
    }

    pub fn __str__(&self) -> String {
        format!("<MemcacheSocket {}>", self.io.fd)
    }

    pub fn get_version(&self) -> u8 {
        self.version
    }

    pub fn set_socket(&mut self, conn: &Bound<'_, PyAny>) -> PyResult<()> {
        self.io.fd = conn.call_method0("fileno")?.extract()?;
        self.io.timeout_ms = get_timeout_ms(conn)?;
        self._conn = conn.clone().unbind();
        self.io.pos = 0;
        self.io.read = 0;
        self.io.noop_expected = 0;
        Ok(())
    }

    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        self._conn.call_method0(py, "close")?;
        self.io.fd = -1;
        self.io.pos = 0;
        self.io.read = 0;
        self.io.noop_expected = 0;
        Ok(())
    }

    /// Send data to the socket, optionally appending a NOOP command.
    /// Releases the GIL during socket I/O.
    pub fn sendall(&mut self, py: Python<'_>, data: &[u8], with_noop: bool) -> PyResult<()> {
        let io = &mut self.io;
        py.detach(|| io.sendall_impl(data, with_noop))
            .map_err(|e| socket_err_io("Error sending data", e))?;
        if with_noop {
            self.io.noop_expected += 1;
        }
        Ok(())
    }

    /// Read and parse the next response header.
    /// Releases the GIL during socket I/O and header parsing.
    pub fn get_response(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let io = &mut self.io;
        let header = py
            .detach(|| io.get_header())
            .map_err(|e| socket_err_io("Error reading header", e))?;

        match header.response_type {
            Some(RESPONSE_VALUE) => {
                let size = header
                    .size
                    .ok_or_else(|| socket_err("Value response missing size"))?;
                let flags = header
                    .flags
                    .ok_or_else(|| socket_err("Value response missing flags"))?;
                into_py(py, Value::new(size, flags, None))
            }
            Some(RESPONSE_SUCCESS) => {
                let flags = header
                    .flags
                    .ok_or_else(|| socket_err("Success response missing flags"))?;
                into_py(py, Success::new(flags))
            }
            Some(RESPONSE_NOT_STORED) => into_py(py, NotStored::new()),
            Some(RESPONSE_CONFLICT) => into_py(py, Conflict::new()),
            Some(RESPONSE_MISS) => into_py(py, Miss::new()),
            _ => Err(socket_err(&format!(
                "Unknown response code: {:?}",
                header.response_type
            ))),
        }
    }

    /// Read value data from the socket.
    /// Releases the GIL during socket I/O.
    /// For the common case (value fits in buffer), creates PyBytes directly
    /// from the buffer — no intermediate allocation.
    pub fn get_value<'py>(&mut self, py: Python<'py>, size: u32) -> PyResult<Bound<'py, PyBytes>> {
        let size_usize = size as usize;
        let io = &mut self.io;

        // Phase 1: recv data without GIL
        let location = py
            .detach(|| io.ensure_value(size_usize))
            .map_err(|e| socket_err_io("Error receiving value", e))?;

        // Phase 2: create PyBytes with GIL
        match location {
            ValueData::InBuffer => {
                // Common path: value is in io.buf — create PyBytes directly, no extra alloc
                let data_start = self.io.pos;
                let data_end = data_start + size_usize;
                let result = PyBytes::new(py, &self.io.buf[data_start..data_end]);
                self.io.pos = data_end + ENDL_LEN;
                Ok(result)
            }
            ValueData::Allocated(data) => {
                // Large value path: data already in Vec, create PyBytes from it
                Ok(PyBytes::new(py, &data))
            }
        }
    }
}
