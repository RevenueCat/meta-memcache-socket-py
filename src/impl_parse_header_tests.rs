#[cfg(test)]
mod tests {
    use crate::constants::*;
    use crate::impl_parse_header::impl_parse_header;

    #[test]
    fn test_no_crnl_in_buffer() {
        let data = b"X\rX\nX";
        assert!(impl_parse_header(data, 0, data.len()).is_none());
    }

    #[test]
    fn test_value_response() {
        let data = b"VA 1234 c1234567 h0 l1111 t2222 f1 Z s3333  MORE_SPACES_ARE_OK_TOO  Ofooonly UNKNOWN FLAGS Ofoobar\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_VALUE));
        assert_eq!(h.size, Some(1234));
        let flags = h.flags.unwrap();
        assert_eq!(flags.cas_token, Some(1_234_567));
        assert_eq!(flags.fetched, Some(false));
        assert_eq!(flags.last_access, Some(1111));
        assert_eq!(flags.ttl, Some(2222));
        assert_eq!(flags.client_flag, Some(1));
        assert_eq!(flags.win, Some(false));
        assert!(!flags.stale);
        assert_eq!(flags.size, Some(3333));
        assert_eq!(flags.opaque, Some(b"foobar".to_vec()));
    }

    #[test]
    fn test_size_int_overflow() {
        let data = b"VA 12345678901234567890 c123456789001234567890 l111 t12345678901234567890\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert!(h.response_type.is_none());
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_flags_int_overflow() {
        let data = b"VA 1234 c123456789001234567890 l111 t12345678901234567890\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_VALUE));
        assert_eq!(h.size, Some(1234));
        let flags = h.flags.unwrap();
        assert!(flags.cas_token.is_none());
        assert!(flags.fetched.is_none());
        assert_eq!(flags.last_access, Some(111));
        assert!(flags.ttl.is_none());
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert!(!flags.stale);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());
    }

    #[test]
    fn test_bad_ttls() {
        let data = b"VA 1234 c111 t\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_VALUE));
        assert_eq!(h.size, Some(1234));
        let flags = h.flags.unwrap();
        assert_eq!(flags.cas_token, Some(111));
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert!(flags.ttl.is_none());
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert!(!flags.stale);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());

        let data = b"VA 1234 t-999 c111\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_VALUE));
        assert_eq!(h.size, Some(1234));
        let flags = h.flags.unwrap();
        assert_eq!(flags.cas_token, Some(111));
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert_eq!(flags.ttl, Some(-1));
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert!(!flags.stale);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());

        let data = b"VA 1234 t- c111\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_VALUE));
        assert_eq!(h.size, Some(1234));
        let flags = h.flags.unwrap();
        assert_eq!(flags.cas_token, Some(111));
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert_eq!(flags.ttl, Some(-1));
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert!(!flags.stale);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());
    }

    #[test]
    fn test_value_response_no_flags() {
        let data = b"VA 1234\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_VALUE));
        assert_eq!(h.size, Some(1234));
        let flags = h.flags.unwrap();
        assert!(flags.cas_token.is_none());
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert!(flags.ttl.is_none());
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert!(!flags.stale);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());
    }

    #[test]
    fn test_value_response_no_size() {
        let data = b"VA c123\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert!(h.response_type.is_none());
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_success_reponse() {
        let data = b"HD c1234567 h0 l1111 t-1 f1 X W s2222 Ofoobar UNKNOWN FLAGS\r\nOK\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len() - 4);
        assert_eq!(h.response_type, Some(RESPONSE_SUCCESS));
        assert!(h.size.is_none());
        let flags = h.flags.unwrap();
        assert_eq!(flags.cas_token, Some(1_234_567));
        assert_eq!(flags.fetched, Some(false));
        assert_eq!(flags.last_access, Some(1111));
        assert_eq!(flags.ttl, Some(-1));
        assert_eq!(flags.client_flag, Some(1));
        assert_eq!(flags.win, Some(true));
        assert!(flags.stale);
        assert_eq!(flags.size, Some(2222));
        assert_eq!(flags.opaque, Some(b"foobar".to_vec()));

        let h = impl_parse_header(data, data.len() - 4, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_SUCCESS));
        let flags = h.flags.unwrap();
        assert!(flags.cas_token.is_none());
        assert!(flags.fetched.is_none());
        assert!(flags.last_access.is_none());
        assert!(flags.ttl.is_none());
        assert!(flags.client_flag.is_none());
        assert!(flags.win.is_none());
        assert!(!flags.stale);
        assert!(flags.size.is_none());
        assert!(flags.opaque.is_none());
    }

    #[test]
    fn test_not_stored_response() {
        let data = b"NS\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_NOT_STORED));
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_conflict_response() {
        let data = b"EX\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_CONFLICT));
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_miss_response() {
        let data = b"EN\r\nNF\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, 4);
        assert_eq!(h.response_type, Some(RESPONSE_MISS));
        assert!(h.size.is_none());
        assert!(h.flags.is_none());

        let h = impl_parse_header(data, 4, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_MISS));
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_noop_response() {
        let data = b"MN\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert_eq!(h.response_type, Some(RESPONSE_NOOP));
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_unknown_response() {
        let data = b"XX 33 c1 Z f1\r\n";
        let h = impl_parse_header(data, 0, data.len()).unwrap();
        assert_eq!(h.end_pos, data.len());
        assert!(h.response_type.is_none());
        assert!(h.size.is_none());
        assert!(h.flags.is_none());
    }

    #[test]
    fn test_response_too_small() {
        let data = b"X\r\n";
        assert!(impl_parse_header(data, 0, data.len()).is_none());
    }

    #[test]
    fn test_end_is_out_of_bounds() {
        let data = b"NOENDLINE";
        assert!(impl_parse_header(data, 0, data.len() + 100).is_none());
    }
}
