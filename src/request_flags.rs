use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::{MA_MODE_INC, SET_MODE_SET};

#[pyclass(eq, skip_from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct RequestFlags {
    #[pyo3(get, set)]
    no_reply: bool,
    #[pyo3(get, set)]
    return_client_flag: bool,
    #[pyo3(get, set)]
    return_cas_token: bool,
    #[pyo3(get, set)]
    return_value: bool,
    #[pyo3(get, set)]
    return_ttl: bool,
    #[pyo3(get, set)]
    return_size: bool,
    #[pyo3(get, set)]
    return_last_access: bool,
    #[pyo3(get, set)]
    return_fetched: bool,
    #[pyo3(get, set)]
    return_key: bool,
    #[pyo3(get, set)]
    no_update_lru: bool,
    #[pyo3(get, set)]
    mark_stale: bool,
    #[pyo3(get, set)]
    cache_ttl: Option<u32>,
    #[pyo3(get, set)]
    recache_ttl: Option<u32>,
    #[pyo3(get, set)]
    vivify_on_miss_ttl: Option<u32>,
    #[pyo3(get, set)]
    client_flag: Option<u32>,
    #[pyo3(get, set)]
    ma_initial_value: Option<u64>,
    #[pyo3(get, set)]
    ma_delta_value: Option<u64>,
    #[pyo3(get, set)]
    cas_token: Option<u32>,
    #[pyo3(get, set)]
    opaque: Option<Vec<u8>>,
    #[pyo3(get, set)]
    mode: Option<u8>,
}

impl RequestFlags {
    /// Check if the no_reply flag is set (crate-internal use).
    pub(crate) fn is_no_reply(&self) -> bool {
        self.no_reply
    }

    pub fn push_bytes(&self, buf: &mut Vec<u8>, allow_no_reply_flag: bool) {
        let mut itoa_buf = itoa::Buffer::new();
        // allow_no_reply_flag controls whether the wire-level `q` flag is emitted
        // for no_reply flag.
        if allow_no_reply_flag && self.no_reply {
            buf.push(b' ');
            buf.push(b'q');
        }
        if self.return_client_flag {
            buf.push(b' ');
            buf.push(b'f');
        }
        if self.return_cas_token {
            buf.push(b' ');
            buf.push(b'c');
        }
        if self.return_value {
            buf.push(b' ');
            buf.push(b'v');
        }
        if self.return_ttl {
            buf.push(b' ');
            buf.push(b't');
        }
        if self.return_size {
            buf.push(b' ');
            buf.push(b's');
        }
        if self.return_last_access {
            buf.push(b' ');
            buf.push(b'l');
        }
        if self.return_fetched {
            buf.push(b' ');
            buf.push(b'h');
        }
        if self.return_key {
            buf.push(b' ');
            buf.push(b'k');
        }
        if self.no_update_lru {
            buf.push(b' ');
            buf.push(b'u');
        }
        if self.mark_stale {
            buf.push(b' ');
            buf.push(b'I');
        }
        if let Some(v) = self.cache_ttl {
            buf.push(b' ');
            buf.push(b'T');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = self.recache_ttl {
            buf.push(b' ');
            buf.push(b'R');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = self.vivify_on_miss_ttl {
            buf.push(b' ');
            buf.push(b'N');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = self.client_flag {
            buf.push(b' ');
            buf.push(b'F');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = self.ma_initial_value {
            buf.push(b' ');
            buf.push(b'J');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = self.ma_delta_value {
            buf.push(b' ');
            buf.push(b'D');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = self.cas_token {
            buf.push(b' ');
            buf.push(b'C');
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        if let Some(v) = &self.opaque {
            buf.push(b' ');
            buf.push(b'O');
            buf.extend_from_slice(v);
        }
        if let Some(v) = self.mode
            && v != SET_MODE_SET
            && v != MA_MODE_INC
        {
            // Set/inc are the default, no need to send them
            buf.push(b' ');
            buf.push(b'M');
            buf.push(v);
        }
    }
}

#[pymethods]
impl RequestFlags {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            /,
            *,
            no_reply=false,
            return_client_flag=false,
            return_cas_token=false,
            return_value=false,
            return_ttl=false,
            return_size=false,
            return_last_access=false,
            return_fetched=false,
            return_key=false,
            no_update_lru=false,
            mark_stale=false,
            cache_ttl=None,
            recache_ttl=None,
            vivify_on_miss_ttl=None,
            client_flag=None,
            ma_initial_value=None,
            ma_delta_value=None,
            cas_token=None,
            opaque=None,
            mode=None
        ),
        text_signature = "(*,
            no_reply: bool = False,
            return_client_flag: bool = False,
            return_cas_token: bool = False,
            return_value = False
            return_ttl: bool = False,
            return_size: bool = False,
            return_last_access: bool = False,
            return_fetched: bool = False,
            return_key: bool = False,
            no_update_lru: bool = False,
            mark_stale: bool = False,
            cache_ttl: Optional[int] = None,
            recache_ttl: Optional[int] = None,
            vivify_on_miss_ttl: Optional[int] = None,
            client_flag: Optional[int] = None,
            ma_initial_value: Optional[int] = None,
            ma_delta_value: Optional[int] = None,
            cas_token: Optional[int] = None,
            opaque: Optional[bytes] = None,
            mode: Optional[int] = None)"
    )]
    pub fn new(
        no_reply: bool,
        return_client_flag: bool,
        return_cas_token: bool,
        return_value: bool,
        return_ttl: bool,
        return_size: bool,
        return_last_access: bool,
        return_fetched: bool,
        return_key: bool,
        no_update_lru: bool,
        mark_stale: bool,
        cache_ttl: Option<u32>,
        recache_ttl: Option<u32>,
        vivify_on_miss_ttl: Option<u32>,
        client_flag: Option<u32>,
        ma_initial_value: Option<u64>,
        ma_delta_value: Option<u64>,
        cas_token: Option<u32>,
        opaque: Option<Vec<u8>>,
        mode: Option<u8>,
    ) -> Self {
        RequestFlags {
            no_reply,
            return_client_flag,
            return_cas_token,
            return_value,
            return_ttl,
            return_size,
            return_last_access,
            return_fetched,
            return_key,
            no_update_lru,
            mark_stale,
            cache_ttl,
            recache_ttl,
            vivify_on_miss_ttl,
            client_flag,
            ma_initial_value,
            ma_delta_value,
            cas_token,
            opaque,
            mode,
        }
    }

    pub fn copy(&self) -> Self {
        self.clone()
    }

    pub fn __str__(&self) -> String {
        format!(
            "RequestFlags(no_reply={:?}, return_client_flag={:?}, return_cas_token={:?}, return_value={:?}, return_ttl={:?}, return_size={:?}, return_last_access={:?}, return_fetched={:?}, return_key={:?}, no_update_lru={:?}, mark_stale={:?}, cache_ttl={:?}, recache_ttl={:?}, vivify_on_miss_ttl={:?}, client_flag={:?}, ma_initial_value={:?}, ma_delta_value={:?}, cas_token={:?}, opaque={:?}, mode={:?})",
            self.no_reply,
            self.return_client_flag,
            self.return_cas_token,
            self.return_value,
            self.return_ttl,
            self.return_size,
            self.return_last_access,
            self.return_fetched,
            self.return_key,
            self.no_update_lru,
            self.mark_stale,
            self.cache_ttl,
            self.recache_ttl,
            self.vivify_on_miss_ttl,
            self.client_flag,
            self.ma_initial_value,
            self.ma_delta_value,
            self.cas_token,
            self.opaque,
            self.mode,
        )
    }

    pub fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let mut flags: Vec<u8> = Vec::with_capacity(64);
        self.push_bytes(&mut flags, /* allow_no_reply_flag */ true);
        PyBytes::new(py, &flags)
    }
}
