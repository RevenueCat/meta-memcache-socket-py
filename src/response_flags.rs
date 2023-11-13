use atoi::FromRadix10Checked;
use pyo3::prelude::*;

#[inline]
fn find_space_or_end(header: &[u8], start: usize) -> usize {
    let mut n = start;
    while n < header.len() {
        if header[n] == b' ' {
            break;
        }
        n += 1;
    }
    n
}

#[inline]
fn get_u32_value(header: &[u8], start: usize) -> (Option<u32>, usize) {
    match u32::from_radix_10_checked(&header[start..]) {
        (Some(v), len) if len > 0 => (Some(v), start + len),
        _ => (None, find_space_or_end(&header, start)),
    }
}

#[inline]
fn get_i32_value(header: &[u8], start: usize) -> (Option<i32>, usize) {
    match i32::from_radix_10_checked(&header[start..]) {
        (Some(v), len) if len > 0 => (Some(v), start + len),
        _ => (None, find_space_or_end(&header, start)),
    }
}

#[pyclass]
pub struct ResponseFlags {
    #[pyo3(get)]
    pub cas_token: Option<u32>,
    #[pyo3(get)]
    pub fetched: Option<bool>,
    #[pyo3(get)]
    pub last_access: Option<u32>,
    #[pyo3(get)]
    pub ttl: Option<i32>,
    #[pyo3(get)]
    pub client_flag: Option<u32>,
    #[pyo3(get)]
    pub win: Option<bool>,
    #[pyo3(get)]
    pub stale: bool,
    #[pyo3(get)]
    pub size: Option<u32>,
    #[pyo3(get)]
    pub opaque: Option<Vec<u8>>,
}

#[pymethods]
impl ResponseFlags {
    #[new]
    #[pyo3(
        signature = (
            /,
            *,
            cas_token=None,
            fetched=None,
            last_access=None,
            ttl=None,
            client_flag=None,
            win=None,
            stale=false,
            size=None,
            opaque=None,
            ),
        text_signature = "(*,
            cas_token=None,
            fetched=None,
            last_access=None,
            ttl=None,
            client_flag=None,
            win=None,
            stale=False,
            size=None,
            opaque=None)"
    )]
    fn new(
        cas_token: Option<u32>,
        fetched: Option<bool>,
        last_access: Option<u32>,
        ttl: Option<i32>,
        client_flag: Option<u32>,
        win: Option<bool>,
        stale: Option<bool>,
        size: Option<u32>,
        opaque: Option<Vec<u8>>,
    ) -> Self {
        ResponseFlags {
            cas_token,
            fetched,
            last_access,
            ttl,
            client_flag,
            win,
            stale: stale.unwrap_or(false),
            size,
            opaque,
        }
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self.cas_token == other.cas_token
            && self.fetched == other.fetched
            && self.last_access == other.last_access
            && self.ttl == other.ttl
            && self.client_flag == other.client_flag
            && self.win == other.win
            && self.stale == other.stale
            && self.size == other.size
            && self.opaque == other.opaque
    }

    pub fn __str__(&self) -> String {
        format!(
            "ResponseFlags(cas_token={:?}, fetched={:?}, last_access={:?}, ttl={:?}, client_flag={:?}, win={:?}, stale={}, size={:?}, opaque={:?})",
            self.cas_token,
            self.fetched,
            self.last_access,
            self.ttl,
            self.client_flag,
            self.win,
            self.stale,
            self.size,
            self.opaque,
        )
    }

    #[staticmethod]
    pub fn from_success_header(header: &[u8]) -> Self {
        return ResponseFlags::parse_flags(header, 3);
    }

    #[staticmethod]
    pub fn from_value_header(header: &[u8]) -> Option<(u32, Self)> {
        let size_start: usize = 3;
        if header.len() < size_start + 1 {
            return None;
        }
        // let (size, pos) = u32::from_radix_10_checked(&header[size_start..]);
        match u32::from_radix_10_checked(&header[size_start..]) {
            (Some(size), pos) if pos > 0 => {
                let flags = ResponseFlags::parse_flags(header, size_start + pos);
                Some((size, flags))
            }
            _ => None,
        }
    }

    #[staticmethod]
    pub fn parse_flags(header: &[u8], start: usize) -> Self {
        let mut cas_token: Option<u32> = None;
        let mut fetched: Option<bool> = None;
        let mut last_access: Option<u32> = None;
        let mut ttl: Option<i32> = None;
        let mut client_flag: Option<u32> = None;
        let mut win: Option<bool> = None;
        let mut stale: bool = false;
        let mut size: Option<u32> = None;
        let mut opaque: Option<Vec<u8>> = None;

        let mut n = start;
        while n < header.len() {
            let flag = header[n];
            // Move past the flag
            n += 1;
            match flag {
                b' ' => {
                    // Space, skip it
                    continue;
                }
                b'c' => {
                    // cas_token flag (u32)
                    (cas_token, n) = get_u32_value(&header, n);
                }
                b'h' => {
                    // fetched flag (bool) encoded as 1 or 0
                    fetched = match header[n] {
                        b'1' => Some(true),
                        b'0' => Some(false),
                        _ => None,
                    };
                    n = find_space_or_end(&header, n + 1);
                }
                b'l' => {
                    // last_access flag (u32)
                    (last_access, n) = get_u32_value(&header, n);
                }
                b't' => {
                    // ttl flag (i32) encoded as -1 (for no ttl) or a positive number
                    if n < header.len() && header[n] == b'-' {
                        ttl = Some(-1);
                        n = find_space_or_end(&header, n)
                    } else {
                        (ttl, n) = get_i32_value(&header, n);
                    }
                }
                b'f' => {
                    // client_flag flag (u32)
                    (client_flag, n) = get_u32_value(&header, n);
                }
                b'W' => {
                    // win flag (bool), no value
                    win = Some(true);
                }
                b'Z' => {
                    // lost flag (bool), no value
                    win = Some(false);
                }
                b'X' => {
                    // stale flag (bool), no value
                    stale = true;
                }
                b's' => {
                    // size flag (u32)
                    (size, n) = get_u32_value(&header, n);
                }
                b'O' => {
                    // opaque flag (bytes)
                    let start = n;
                    n = find_space_or_end(&header, start);
                    opaque = Some(header[start..n].to_vec());
                }
                _ => {
                    // Unknown flag, skip it
                    n = find_space_or_end(&header, n);
                }
            }
            // n points now to a space, so continue past it
            n += 1;
        }
        ResponseFlags {
            cas_token,
            fetched,
            last_access,
            ttl,
            client_flag,
            win,
            stale,
            size,
            opaque,
        }
    }
}
