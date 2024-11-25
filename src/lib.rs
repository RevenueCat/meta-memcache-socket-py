mod constants;
mod impl_build_cmd;
mod impl_build_cmd_tests;
mod impl_parse_header;
mod impl_parse_header_tests;
mod request_flags;
mod response_flags;
pub use constants::*;
use impl_build_cmd::impl_build_cmd;
use impl_parse_header::impl_parse_header;
pub use request_flags::RequestFlags;
pub use response_flags::ResponseFlags;

use std::slice;

use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyfunction]
#[pyo3(
    signature = (
        buffer,
        start,
        end,
    ),
    text_signature = "(buffer: Union[memoryview, bytearray], start: int, end: int)",
)]
pub fn parse_header(
    buffer: PyBuffer<u8>,
    start: usize,
    end: usize,
) -> PyResult<Option<(usize, Option<u8>, Option<u32>, Option<ResponseFlags>)>> {
    if end > buffer.len_bytes() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "End must be less than buffer length",
        ));
    }
    let data = unsafe { slice::from_raw_parts(buffer.buf_ptr() as *const u8, buffer.len_bytes()) };
    Ok(impl_parse_header(data, start, end))
}

#[pyfunction]
#[pyo3(
    signature = (
        cmd,
        key,
        size=None,
        request_flags=None,
        legacy_size_format=false,
    ),
    text_signature = "(cmd: bytes, key: bytes, size: Optional[int], request_flags: Optional[RequestFlags], legacy_size_format: bool = False)",
)]
pub fn build_cmd(
    py: Python,
    cmd: &[u8],
    key: &[u8],
    size: Option<u32>,
    request_flags: Option<&RequestFlags>,
    legacy_size_format: bool,
) -> PyResult<Py<PyBytes>> {
    match impl_build_cmd(cmd, key, size, request_flags, legacy_size_format) {
        Some(buf) => Ok(PyBytes::new(py, &buf).into()),
        None => Err(pyo3::exceptions::PyValueError::new_err("Key is too long")),
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        key,
        request_flags=None,
    ),
    text_signature = "(key: bytes, request_flags: Optional[RequestFlags])",
)]
pub fn build_meta_get(
    py: Python,
    key: &[u8],
    request_flags: Option<&RequestFlags>,
) -> PyResult<Py<PyBytes>> {
    match impl_build_cmd(b"mg", key, None, request_flags, false) {
        Some(buf) => Ok(PyBytes::new(py, &buf).into()),
        None => Err(pyo3::exceptions::PyValueError::new_err("Key is too long")),
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        key,
        size,
        request_flags=None,
        legacy_size_format=false,
    ),
    text_signature = "(key: bytes, size: int, request_flags: Optional[RequestFlags], legacy_size_format: bool = False)",
)]
pub fn build_meta_set(
    py: Python,
    key: &[u8],
    size: u32,
    request_flags: Option<&RequestFlags>,
    legacy_size_format: bool,
) -> PyResult<Py<PyBytes>> {
    match impl_build_cmd(b"ms", key, Some(size), request_flags, legacy_size_format) {
        Some(buf) => Ok(PyBytes::new(py, &buf).into()),
        None => Err(pyo3::exceptions::PyValueError::new_err("Key is too long")),
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        key,
        request_flags=None,
    ),
    text_signature = "(key: bytes, request_flags: Optional[RequestFlags])",
)]
pub fn build_meta_delete(
    py: Python,
    key: &[u8],
    request_flags: Option<&RequestFlags>,
) -> PyResult<Py<PyBytes>> {
    match impl_build_cmd(b"md", key, None, request_flags, false) {
        Some(buf) => Ok(PyBytes::new(py, &buf).into()),
        None => Err(pyo3::exceptions::PyValueError::new_err("Key is too long")),
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        key,
        request_flags=None,
    ),
    text_signature = "(key: bytes, request_flags: Optional[RequestFlags])",
)]
pub fn build_meta_arithmetic(
    py: Python,
    key: &[u8],
    request_flags: Option<&RequestFlags>,
) -> PyResult<Py<PyBytes>> {
    match impl_build_cmd(b"ma", key, None, request_flags, false) {
        Some(buf) => Ok(PyBytes::new(py, &buf).into()),
        None => Err(pyo3::exceptions::PyValueError::new_err("Key is too long")),
    }
}

#[pymodule]
fn meta_memcache_socket(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ResponseFlags>()?;
    module.add_class::<RequestFlags>()?;
    module.add_function(wrap_pyfunction!(parse_header, module)?)?;
    module.add_function(wrap_pyfunction!(build_cmd, module)?)?;
    module.add_function(wrap_pyfunction!(build_meta_get, module)?)?;
    module.add_function(wrap_pyfunction!(build_meta_set, module)?)?;
    module.add_function(wrap_pyfunction!(build_meta_delete, module)?)?;
    module.add_function(wrap_pyfunction!(build_meta_arithmetic, module)?)?;
    module.add("RESPONSE_VALUE", RESPONSE_VALUE)?;
    module.add("RESPONSE_SUCCESS", RESPONSE_SUCCESS)?;
    module.add("RESPONSE_NOT_STORED", RESPONSE_NOT_STORED)?;
    module.add("RESPONSE_CONFLICT", RESPONSE_CONFLICT)?;
    module.add("RESPONSE_MISS", RESPONSE_MISS)?;
    module.add("RESPONSE_NOOP", RESPONSE_NOOP)?;
    module.add("SET_MODE_ADD", SET_MODE_ADD)?;
    module.add("SET_MODE_APPEND", SET_MODE_APPEND)?;
    module.add("SET_MODE_PREPEND", SET_MODE_PREPEND)?;
    module.add("SET_MODE_REPLACE", SET_MODE_REPLACE)?;
    module.add("SET_MODE_SET", SET_MODE_SET)?;
    module.add("MA_MODE_INC", MA_MODE_INC)?;
    module.add("MA_MODE_DEC", MA_MODE_DEC)?;
    Ok(())
}
