#[cfg(test)]
mod tests {
    use crate::response_flags::ResponseFlags;

    #[test]
    fn test_parse_flags_empty() {
        let flags = ResponseFlags::parse_flags(b"HD", 2);
        assert_eq!(flags.cas_token, None);
        assert_eq!(flags.fetched, None);
        assert_eq!(flags.last_access, None);
        assert_eq!(flags.ttl, None);
        assert_eq!(flags.client_flag, None);
        assert_eq!(flags.win, None);
        assert!(!flags.stale);
        assert_eq!(flags.size, None);
        assert_eq!(flags.opaque, None);
    }

    #[test]
    fn test_parse_cas_token() {
        let flags = ResponseFlags::parse_flags(b"HD c12345", 2);
        assert_eq!(flags.cas_token, Some(12345));
    }

    #[test]
    fn test_parse_fetched_true() {
        let flags = ResponseFlags::parse_flags(b"HD h1", 2);
        assert_eq!(flags.fetched, Some(true));
    }

    #[test]
    fn test_parse_fetched_false() {
        let flags = ResponseFlags::parse_flags(b"HD h0", 2);
        assert_eq!(flags.fetched, Some(false));
    }

    #[test]
    fn test_parse_fetched_invalid() {
        let flags = ResponseFlags::parse_flags(b"HD hX", 2);
        assert_eq!(flags.fetched, None);
    }

    #[test]
    fn test_parse_last_access() {
        let flags = ResponseFlags::parse_flags(b"HD l999", 2);
        assert_eq!(flags.last_access, Some(999));
    }

    #[test]
    fn test_parse_ttl_positive() {
        let flags = ResponseFlags::parse_flags(b"HD t3600", 2);
        assert_eq!(flags.ttl, Some(3600));
    }

    #[test]
    fn test_parse_ttl_negative_one() {
        // -1 means no expiration
        let flags = ResponseFlags::parse_flags(b"HD t-1", 2);
        assert_eq!(flags.ttl, Some(-1));
    }

    #[test]
    fn test_parse_ttl_negative_other() {
        // Any negative value is treated as -1
        let flags = ResponseFlags::parse_flags(b"HD t-999", 2);
        assert_eq!(flags.ttl, Some(-1));
    }

    #[test]
    fn test_parse_ttl_just_dash() {
        let flags = ResponseFlags::parse_flags(b"HD t-", 2);
        assert_eq!(flags.ttl, Some(-1));
    }

    #[test]
    fn test_parse_client_flag() {
        let flags = ResponseFlags::parse_flags(b"HD f42", 2);
        assert_eq!(flags.client_flag, Some(42));
    }

    #[test]
    fn test_parse_win() {
        let flags = ResponseFlags::parse_flags(b"HD W", 2);
        assert_eq!(flags.win, Some(true));
    }

    #[test]
    fn test_parse_lose() {
        let flags = ResponseFlags::parse_flags(b"HD Z", 2);
        assert_eq!(flags.win, Some(false));
    }

    #[test]
    fn test_parse_stale() {
        let flags = ResponseFlags::parse_flags(b"HD X", 2);
        assert!(flags.stale);
    }

    #[test]
    fn test_parse_size() {
        let flags = ResponseFlags::parse_flags(b"HD s4096", 2);
        assert_eq!(flags.size, Some(4096));
    }

    #[test]
    fn test_parse_opaque() {
        let flags = ResponseFlags::parse_flags(b"HD Otoken123", 2);
        assert_eq!(flags.opaque, Some(b"token123".to_vec()));
    }

    #[test]
    fn test_parse_opaque_with_more_flags() {
        let flags = ResponseFlags::parse_flags(b"HD Otoken X", 2);
        assert_eq!(flags.opaque, Some(b"token".to_vec()));
        assert!(flags.stale);
    }

    #[test]
    fn test_parse_unknown_flag_skipped() {
        let flags = ResponseFlags::parse_flags(b"HD Q123 c99", 2);
        assert_eq!(flags.cas_token, Some(99));
    }

    #[test]
    fn test_parse_multiple_spaces() {
        let flags = ResponseFlags::parse_flags(b"HD  c1  h1  ", 2);
        assert_eq!(flags.cas_token, Some(1));
        assert_eq!(flags.fetched, Some(true));
    }

    #[test]
    fn test_parse_all_flags() {
        let flags = ResponseFlags::parse_flags(b"HD c100 h1 l200 t300 f400 W X s500 Odata", 2);
        assert_eq!(flags.cas_token, Some(100));
        assert_eq!(flags.fetched, Some(true));
        assert_eq!(flags.last_access, Some(200));
        assert_eq!(flags.ttl, Some(300));
        assert_eq!(flags.client_flag, Some(400));
        assert_eq!(flags.win, Some(true));
        assert!(flags.stale);
        assert_eq!(flags.size, Some(500));
        assert_eq!(flags.opaque, Some(b"data".to_vec()));
    }

    #[test]
    fn test_parse_u32_overflow() {
        // u32 max is 4294967295, this overflows
        let flags = ResponseFlags::parse_flags(b"HD c99999999999", 2);
        assert_eq!(flags.cas_token, None);
    }

    #[test]
    fn test_parse_zero_values() {
        let flags = ResponseFlags::parse_flags(b"HD c0 l0 t0 f0 s0", 2);
        assert_eq!(flags.cas_token, Some(0));
        assert_eq!(flags.last_access, Some(0));
        assert_eq!(flags.ttl, Some(0));
        assert_eq!(flags.client_flag, Some(0));
        assert_eq!(flags.size, Some(0));
    }

    // from_value_header tests
    #[test]
    fn test_from_value_header_basic() {
        let result = ResponseFlags::from_value_header(b"VA 100 c1");
        assert!(result.is_some());
        let (size, flags) = result.unwrap();
        assert_eq!(size, 100);
        assert_eq!(flags.cas_token, Some(1));
    }

    #[test]
    fn test_from_value_header_no_flags() {
        let result = ResponseFlags::from_value_header(b"VA 42");
        assert!(result.is_some());
        let (size, flags) = result.unwrap();
        assert_eq!(size, 42);
        assert_eq!(flags.cas_token, None);
    }

    #[test]
    fn test_from_value_header_too_short() {
        assert!(ResponseFlags::from_value_header(b"VA").is_none());
        assert!(ResponseFlags::from_value_header(b"VA ").is_none());
    }

    #[test]
    fn test_from_value_header_no_size() {
        // No numeric size after "VA "
        assert!(ResponseFlags::from_value_header(b"VA c1").is_none());
    }

    #[test]
    fn test_from_value_header_size_overflow() {
        assert!(ResponseFlags::from_value_header(b"VA 99999999999").is_none());
    }

    // from_success_header tests
    #[test]
    fn test_from_success_header_basic() {
        let flags = ResponseFlags::from_success_header(b"HD c42 X");
        assert_eq!(flags.cas_token, Some(42));
        assert!(flags.stale);
    }

    #[test]
    fn test_from_success_header_empty() {
        let flags = ResponseFlags::from_success_header(b"HD");
        assert_eq!(flags.cas_token, None);
        assert!(!flags.stale);
    }

    // Last opaque wins when multiple are present
    #[test]
    fn test_last_opaque_wins() {
        let flags = ResponseFlags::parse_flags(b"HD Ofirst Osecond", 2);
        assert_eq!(flags.opaque, Some(b"second".to_vec()));
    }

    // Stale + lose combination
    #[test]
    fn test_stale_and_lose() {
        let flags = ResponseFlags::parse_flags(b"HD X Z", 2);
        assert!(flags.stale);
        assert_eq!(flags.win, Some(false));
    }

    // Win overrides lose (last one wins)
    #[test]
    fn test_win_after_lose() {
        let flags = ResponseFlags::parse_flags(b"HD Z W", 2);
        assert_eq!(flags.win, Some(true));
    }

    #[test]
    fn test_lose_after_win() {
        let flags = ResponseFlags::parse_flags(b"HD W Z", 2);
        assert_eq!(flags.win, Some(false));
    }
}
