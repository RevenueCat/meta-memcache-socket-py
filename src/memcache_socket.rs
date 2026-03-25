use std::os::fd::RawFd;

use log::warn;

use pyo3::BoundObject;
use pyo3::exceptions::{PyConnectionError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::constants::*;
use crate::encode_key::extract_key;
use crate::impl_build_cmd::{BuiltCmd, impl_build_cmd};
use crate::impl_parse_header::{ParsedHeader, impl_parse_header};
use crate::request_flags::RequestFlags;
use crate::response_flags::ResponseFlags;
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
    loop {
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
                continue;
            }
            return Err(err);
        } else if ret == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out",
            ));
        } else if pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "poll error on socket",
            ));
        } else {
            return Ok(());
        }
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

/// Send multiple buffers in a single writev() syscall.
/// Falls back to send_all() for partial writes.
#[inline]
fn send_iovecs(fd: RawFd, slices: &[&[u8]], timeout_ms: libc::c_int) -> Result<(), std::io::Error> {
    let total_len: usize = slices.iter().map(|s| s.len()).sum();
    if total_len == 0 {
        return Ok(());
    }

    let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(slices.len());
    for slice in slices {
        iovecs.push(libc::iovec {
            iov_base: slice.as_ptr() as *mut libc::c_void,
            iov_len: slice.len(),
        });
    }

    // SAFETY: iovecs entries point to valid byte slices for the duration of writev
    let n = unsafe { libc::writev(fd, iovecs.as_ptr(), iovecs.len() as i32) };
    let written = if n >= 0 {
        n as usize
    } else {
        let err = std::io::Error::last_os_error();
        match err.kind() {
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => 0,
            _ => return Err(err),
        }
    };

    if written >= total_len {
        return Ok(());
    }

    // Partial write: concatenate remaining and send_all
    let mut combined: Vec<u8> = Vec::with_capacity(total_len - written);
    let mut skip = written;
    for slice in slices {
        if skip >= slice.len() {
            skip -= slice.len();
        } else {
            combined.extend_from_slice(&slice[skip..]);
            skip = 0;
        }
    }
    send_all(fd, &combined, timeout_ms)
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
    /// Value is in io.buf starting at this position, for `size` bytes.
    /// pos has already been advanced past the value and ENDL.
    InBuffer(usize),
    /// Value was too large for the buffer, stored in this Vec.
    Allocated(Vec<u8>),
}

enum CmdResult {
    NoReply,
    Response((ParsedHeader, Option<ValueData>)),
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
        if self.read == self.pos {
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

    fn send_cmd(&mut self, cmd: &[u8], with_noop: bool) -> Result<(), std::io::Error> {
        if with_noop {
            send_iovecs(self.fd, &[cmd, NOOP_CMD], self.timeout_ms)?;
            self.noop_expected += 1;
        } else {
            send_all(self.fd, cmd, self.timeout_ms)?;
        }
        Ok(())
    }

    fn send_cmd_with_value(
        &mut self,
        cmd: &[u8],
        value: &[u8],
        with_noop: bool,
    ) -> Result<(), std::io::Error> {
        if with_noop {
            send_iovecs(self.fd, &[cmd, value, ENDL, NOOP_CMD], self.timeout_ms)?;
            self.noop_expected += 1;
        } else {
            send_iovecs(self.fd, &[cmd, value, ENDL], self.timeout_ms)?;
        }
        Ok(())
    }

    /// Ensure value data is available for reading.
    /// Advances pos past the value and ENDL on success.
    ///
    /// For the common case (value fits in buffer), returns InBuffer(start) —
    /// the data is at buf[start..start+size].
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

        let data_start = self.pos;

        if data_in_buf >= message_size {
            // Value + ENDL fully in buffer — validate ENDL in place
            let data_end = data_start + size;
            if self.buf[data_end] != b'\r' || self.buf[data_end + 1] != b'\n' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Value not terminated with \\r\\n",
                ));
            }
            self.pos = data_end + ENDL_LEN;
            Ok(ValueData::InBuffer(data_start))
        } else if data_in_buf >= size {
            // Value in buffer but ENDL partially/not in buffer.
            // Read and validate the ENDL from buffer/socket.
            let data_end = data_start + size;
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
            // ENDL was consumed from buffer/socket directly; buffer is fully consumed
            self.pos = self.read;
            Ok(ValueData::InBuffer(data_start))
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

    /// Read and parse the next response header, including value data for
    /// Value responses. All socket I/O happens in this method (no GIL needed).
    fn get_response_with_value(
        &mut self,
    ) -> Result<(ParsedHeader, Option<ValueData>), std::io::Error> {
        let header = self.get_header()?;
        let value_data = if header.response_type == Some(RESPONSE_VALUE) {
            let size = header.size.unwrap_or(0) as usize;
            Some(self.ensure_value(size)?)
        } else {
            None
        };
        Ok((header, value_data))
    }
}

#[pyclass]
pub struct MemcacheSocket {
    io: SocketIO,
    /// Hold a reference to the Python socket to prevent GC.
    _conn: Py<PyAny>,
    version: u8,
}

/// Private helpers
impl MemcacheSocket {
    fn build_cmd<'py>(
        &self,
        cmd: &[u8],
        key: &'py Bound<'py, PyAny>,
        size: Option<u32>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<BuiltCmd> {
        let key = extract_key(key)?;
        let legacy_size_format = cmd == b"ms" && self.version == SERVER_VERSION_AWS_1_6_6;
        let allow_no_reply_flag = cmd != b"mg";
        impl_build_cmd(
            cmd,
            key,
            size,
            request_flags,
            legacy_size_format,
            allow_no_reply_flag,
        )
        .ok_or_else(|| PyValueError::new_err("Key is empty"))
    }

    /// Convert a parsed header + optional value data into a Python response object.
    fn make_response(
        &self,
        py: Python<'_>,
        header: ParsedHeader,
        value_data: Option<ValueData>,
    ) -> PyResult<Py<PyAny>> {
        match header.response_type {
            Some(RESPONSE_VALUE) => {
                let size = header
                    .size
                    .ok_or_else(|| socket_err("Value response missing size"))?;
                let flags = header
                    .flags
                    .ok_or_else(|| socket_err("Value response missing flags"))?;
                let py_bytes = match value_data {
                    Some(ValueData::InBuffer(start)) => {
                        PyBytes::new(py, &self.io.buf[start..start + size as usize])
                    }
                    Some(ValueData::Allocated(data)) => PyBytes::new(py, &data),
                    None => PyBytes::new(py, b""),
                };
                into_py(
                    py,
                    Value::new(size, flags, Some(py_bytes.into_any().unbind())),
                )
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

    /// Create a Success response with empty flags (for no_reply commands).
    fn success_no_reply(py: Python<'_>) -> PyResult<Py<PyAny>> {
        let flags = ResponseFlags {
            cas_token: None,
            fetched: None,
            last_access: None,
            ttl: None,
            client_flag: None,
            win: None,
            stale: false,
            size: None,
            opaque: None,
        };
        into_py(py, Success::new(flags))
    }
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
            // Non-fatal: the socket will still work with the kernel default buffer size.
            warn!(
                "SO_RCVBUF setsockopt failed (fd={}, requested={}), using kernel default",
                fd, buffer_size
            );
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

    // -----------------------------------------------------------------------
    // Low-level: sendall + get_response
    // -----------------------------------------------------------------------

    /// Send raw data to the socket, optionally appending a NOOP command.
    /// Releases the GIL during socket I/O.
    pub fn sendall(&mut self, py: Python<'_>, data: &[u8], with_noop: bool) -> PyResult<()> {
        let io = &mut self.io;
        py.detach(|| io.send_cmd(data, with_noop))
            .map_err(|e| socket_err_io("Error sending data", e))?;
        Ok(())
    }

    /// Read and parse the next response, including value data for Value responses.
    /// For Value responses, `.value` is set to the raw bytes from the wire.
    /// Releases the GIL during socket I/O.
    pub fn get_response(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let io = &mut self.io;
        let (header, value_data) = py
            .detach(|| io.get_response_with_value())
            .map_err(|e| socket_err_io("Error reading response", e))?;
        self.make_response(py, header, value_data)
    }

    // -----------------------------------------------------------------------
    // Tier 1: send_meta_* (for pipelining — send only, read later)
    // -----------------------------------------------------------------------

    /// Send a meta get command. Use get_response() to read the result later.
    /// Note: no_reply on mg only suppresses misses, hits still return data,
    /// so noop is never injected for get commands.
    #[pyo3(signature = (key, request_flags=None))]
    pub fn send_meta_get(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<()> {
        let cmd = self.build_cmd(b"mg", key, None, request_flags)?;
        if cmd.no_reply {
            return Err(socket_err(
                "internal error: build_cmd produced no_reply=true for mg command",
            ));
        }
        let io = &mut self.io;
        py.detach(|| io.send_cmd(&cmd.buf, false))
            .map_err(|e| socket_err_io("Error sending meta get", e))?;
        Ok(())
    }

    /// Send a meta set command with value. Use get_response() to read the result later.
    /// Uses writev() to send cmd + value + ENDL in a single syscall (zero concatenation).
    /// If no_reply is set, automatically appends a NOOP command.
    #[pyo3(signature = (key, value, request_flags=None))]
    pub fn send_meta_set(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &[u8],
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<()> {
        let cmd = self.build_cmd(b"ms", key, Some(value.len() as u32), request_flags)?;
        let io = &mut self.io;
        py.detach(|| io.send_cmd_with_value(&cmd.buf, value, cmd.no_reply))
            .map_err(|e| socket_err_io("Error sending meta set", e))?;
        Ok(())
    }

    /// Send a meta delete command. Use get_response() to read the result later.
    /// If no_reply is set, automatically appends a NOOP command.
    #[pyo3(signature = (key, request_flags=None))]
    pub fn send_meta_delete(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<()> {
        let cmd = self.build_cmd(b"md", key, None, request_flags)?;
        let io = &mut self.io;
        py.detach(|| io.send_cmd(&cmd.buf, cmd.no_reply))
            .map_err(|e| socket_err_io("Error sending meta delete", e))?;
        Ok(())
    }

    /// Send a meta arithmetic command. Use get_response() to read the result later.
    /// If no_reply is set, automatically appends a NOOP command.
    #[pyo3(signature = (key, request_flags=None))]
    pub fn send_meta_arithmetic(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<()> {
        let cmd = self.build_cmd(b"ma", key, None, request_flags)?;
        let io = &mut self.io;
        py.detach(|| io.send_cmd(&cmd.buf, cmd.no_reply))
            .map_err(|e| socket_err_io("Error sending meta arithmetic", e))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tier 2: meta_* (blocking — send + recv in one call)
    // -----------------------------------------------------------------------

    /// Send a meta get command and return the response.
    /// The entire send + recv happens in a single GIL-released block.
    #[pyo3(signature = (key, request_flags=None))]
    pub fn meta_get(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = self.build_cmd(b"mg", key, None, request_flags)?;
        if cmd.no_reply {
            return Err(socket_err(
                "internal error: build_cmd produced no_reply=true for mg command",
            ));
        }
        let io = &mut self.io;
        let (header, value_data) = py
            .detach(|| {
                io.send_cmd(&cmd.buf, false)?;
                io.get_response_with_value()
            })
            .map_err(|e| socket_err_io("Error in meta_get", e))?;
        self.make_response(py, header, value_data)
    }

    /// Send a meta set command with value and return the response.
    /// For no_reply commands, sends with NOOP and returns Success immediately.
    /// Otherwise, the entire send + recv happens in a single GIL-released block.
    #[pyo3(signature = (key, value, request_flags=None))]
    pub fn meta_set(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &[u8],
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = self.build_cmd(b"ms", key, Some(value.len() as u32), request_flags)?;
        let io = &mut self.io;
        let result = py
            .detach(|| {
                io.send_cmd_with_value(&cmd.buf, value, cmd.no_reply)?;
                if cmd.no_reply {
                    Ok(CmdResult::NoReply)
                } else {
                    Ok(CmdResult::Response(io.get_response_with_value()?))
                }
            })
            .map_err(|e| socket_err_io("Error in meta_set", e))?;
        match result {
            CmdResult::NoReply => Self::success_no_reply(py),
            CmdResult::Response((header, value_data)) => self.make_response(py, header, value_data),
        }
    }

    /// Send a meta delete command and return the response.
    /// For no_reply commands, sends with NOOP and returns Success immediately.
    #[pyo3(signature = (key, request_flags=None))]
    pub fn meta_delete(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = self.build_cmd(b"md", key, None, request_flags)?;
        let io = &mut self.io;
        let result = py
            .detach(|| {
                io.send_cmd(&cmd.buf, cmd.no_reply)?;
                if cmd.no_reply {
                    Ok(CmdResult::NoReply)
                } else {
                    Ok(CmdResult::Response(io.get_response_with_value()?))
                }
            })
            .map_err(|e| socket_err_io("Error in meta_delete", e))?;
        match result {
            CmdResult::NoReply => Self::success_no_reply(py),
            CmdResult::Response((header, value_data)) => self.make_response(py, header, value_data),
        }
    }

    /// Send a meta arithmetic command and return the response.
    /// For no_reply commands, sends with NOOP and returns Success immediately.
    #[pyo3(signature = (key, request_flags=None))]
    pub fn meta_arithmetic(
        &mut self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        request_flags: Option<&RequestFlags>,
    ) -> PyResult<Py<PyAny>> {
        let cmd = self.build_cmd(b"ma", key, None, request_flags)?;
        let io = &mut self.io;
        let result = py
            .detach(|| {
                io.send_cmd(&cmd.buf, cmd.no_reply)?;
                if cmd.no_reply {
                    Ok(CmdResult::NoReply)
                } else {
                    Ok(CmdResult::Response(io.get_response_with_value()?))
                }
            })
            .map_err(|e| socket_err_io("Error in meta_arithmetic", e))?;
        match result {
            CmdResult::NoReply => Self::success_no_reply(py),
            CmdResult::Response((header, value_data)) => self.make_response(py, header, value_data),
        }
    }
}
