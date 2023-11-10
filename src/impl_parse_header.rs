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
    let mut n = start + 2;
    while n < end - 1 {
        if data[n] == b'\r' && data[n + 1] == b'\n' {
            let endl_pos = n + 2;
            match &data[start..start + 2] {
                b"VA" => {
                    match ResponseFlags::from_value_header(&data[start..n]) {
                        Some((size, flags)) => {
                            return Some((endl_pos, Some(RESPONSE_VALUE), Some(size), Some(flags)));
                        }
                        None => {
                            return Some((endl_pos, None, None, None));
                        }
                    };
                }
                b"HD" | b"OK" => {
                    let flags = ResponseFlags::from_success_header(&data[start..n]);
                    return Some((endl_pos, Some(RESPONSE_SUCCESS), None, Some(flags)));
                }
                b"NS" => {
                    return Some((endl_pos, Some(RESPONSE_NOT_STORED), None, None));
                }
                b"EX" => {
                    return Some((endl_pos, Some(RESPONSE_CONFLICT), None, None));
                }
                b"EN" | b"NF" => {
                    return Some((endl_pos, Some(RESPONSE_MISS), None, None));
                }
                b"MN" => {
                    return Some((endl_pos, Some(RESPONSE_NOOP), None, None));
                }
                _ => {
                    return Some((endl_pos, None, None, None));
                }
            }
        }
        n += 1;
    }
    None
}
