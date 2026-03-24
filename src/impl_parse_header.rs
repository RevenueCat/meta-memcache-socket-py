use memchr::memmem;

use crate::{constants::*, ResponseFlags};

pub fn impl_parse_header(
    data: &[u8],
    start: usize,
    end: usize,
) -> Option<(usize, Option<u8>, Option<u32>, Option<ResponseFlags>)> {
    if end - start < 4 {
        return None;
    }
    let end = end.min(data.len());
    let search_start = start + 2;
    if search_start >= end {
        return None;
    }
    // Use memmem SIMD-accelerated search for \r\n
    if let Some(pos) = memmem::find(&data[search_start..end], b"\r\n") {
        let n = search_start + pos;
        let endl_pos = n + 2;
        match &data[start..start + 2] {
            b"VA" => match ResponseFlags::from_value_header(&data[start..n]) {
                Some((size, flags)) => {
                    Some((endl_pos, Some(RESPONSE_VALUE), Some(size), Some(flags)))
                }
                None => Some((endl_pos, None, None, None)),
            },
            b"HD" | b"OK" => {
                let flags = ResponseFlags::from_success_header(&data[start..n]);
                Some((endl_pos, Some(RESPONSE_SUCCESS), None, Some(flags)))
            }
            b"NS" => Some((endl_pos, Some(RESPONSE_NOT_STORED), None, None)),
            b"EX" => Some((endl_pos, Some(RESPONSE_CONFLICT), None, None)),
            b"EN" | b"NF" => Some((endl_pos, Some(RESPONSE_MISS), None, None)),
            b"MN" => Some((endl_pos, Some(RESPONSE_NOOP), None, None)),
            _ => Some((endl_pos, None, None, None)),
        }
    } else {
        None
    }
}
