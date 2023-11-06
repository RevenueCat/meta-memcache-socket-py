use crate::{constants::*, ResponseFlags};

pub fn impl_parse_header(
    data: &[u8],
    start: usize,
    end: usize,
) -> Option<(usize, Option<i32>, Option<u32>, Option<ResponseFlags>)> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_crnl_in_buffer() {
        let data = b"X\rX\nX";
        let no_result = impl_parse_header(data, 0, data.len());
        assert!(no_result.is_none());
    }
    #[test]
    fn test_value_response() {
        let data = b"VA 1234 c1234567 h0 l1111 t2222 f1 Z s3333  MORE_SPACES_ARE_OK_TOO  Ofoobar UNKNOWN FLAGS\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_VALUE));
        assert_eq!(size, Some(1234));
        assert!(flags.is_some());
        let flags = flags.unwrap();
        assert_eq!(flags.cas_token, Some(1234567));
        assert_eq!(flags.fetched, Some(false));
        assert_eq!(flags.last_access, Some(1111));
        assert_eq!(flags.ttl, Some(2222));
        assert_eq!(flags.client_flag, Some(1));
        assert_eq!(flags.win, Some(false));
        assert_eq!(flags.stale, false);
        assert_eq!(flags.size, Some(3333));
        assert_eq!(flags.opaque, Some(b"foobar".to_vec()));
    }

    #[test]
    fn test_value_response_no_flags() {
        let data = b"VA 1234\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_VALUE));
        assert_eq!(size, Some(1234));
        assert!(flags.is_some());
        let flags = flags.unwrap();
        assert!(flags.cas_token.is_none());
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert!(flags.ttl.is_none());
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert_eq!(flags.stale, false);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());
    }

    #[test]
    fn test_value_response_no_size() {
        let data = b"VA c123\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert!(response_type.is_none());
        assert!(size.is_none());
        assert!(flags.is_none());
    }

    #[test]
    fn test_success_reponse() {
        let data = b"HD c1234567 h0 l1111 t2222 f1 X W s3333 Ofoobar UNKNOWN FLAGS\r\nOK\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len() - 4);
        assert_eq!(response_type, Some(RESPONSE_SUCCESS));
        assert!(size.is_none());
        assert!(flags.is_some());
        let flags = flags.unwrap();
        assert_eq!(flags.cas_token, Some(1234567));
        assert_eq!(flags.fetched, Some(false));
        assert_eq!(flags.last_access, Some(1111));
        assert_eq!(flags.ttl, Some(2222));
        assert_eq!(flags.client_flag, Some(1));
        assert_eq!(flags.win, Some(true));
        assert_eq!(flags.stale, true);
        assert_eq!(flags.size, Some(3333));
        assert_eq!(flags.opaque, Some(b"foobar".to_vec()));
        let (end_pos, response_type, size, flags) =
            impl_parse_header(data, data.len() - 4, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_SUCCESS));
        assert!(size.is_none());
        assert!(flags.is_some());
        let flags = flags.unwrap();
        assert!(flags.cas_token.is_none());
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert!(flags.ttl.is_none());
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert_eq!(flags.stale, false);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());
    }

    #[test]
    fn test_not_stored_response() {
        let data = b"NS\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_NOT_STORED));
        assert!(size.is_none());
        assert!(flags.is_none());
    }
    #[test]
    fn test_conflict_response() {
        let data = b"EX\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_CONFLICT));
        assert!(size.is_none());
        assert!(flags.is_none());
    }
    #[test]
    fn test_miss_response() {
        let data = b"EN\r\nNF\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, 4); // Only reads the first response header
        assert_eq!(response_type, Some(RESPONSE_MISS));
        assert!(size.is_none());
        assert!(flags.is_none());
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 4, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_MISS));
        assert!(size.is_none());
        assert!(flags.is_none());
    }
    #[test]
    fn test_noop_response() {
        let data = b"MN\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert_eq!(response_type, Some(RESPONSE_NOOP));
        assert!(size.is_none());
        assert!(flags.is_none());
    }

    #[test]
    fn test_unknown_response() {
        let data = b"XX 33 c1 Z f1\r\n";
        let (end_pos, response_type, size, flags) = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(end_pos, data.len());
        assert!(response_type.is_none());
        assert!(size.is_none());
        assert!(flags.is_none());
    }

    #[test]
    fn test_response_too_small() {
        let data = b"X\r\n";
        let no_result = impl_parse_header(data, 0, data.len());
        assert!(no_result.is_none());
    }

    #[test]
    fn test_end_is_out_of_bounds() {
        let data = b"NOENDLINE";
        let no_result = impl_parse_header(data, 0, data.len() + 100);
        assert!(no_result.is_none());
    }
}
