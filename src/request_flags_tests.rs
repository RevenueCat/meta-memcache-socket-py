#[cfg(test)]
mod tests {
    use crate::request_flags::RequestFlags;
    use crate::{MA_MODE_DEC, MA_MODE_INC, SET_MODE_ADD, SET_MODE_APPEND, SET_MODE_SET};

    fn default_flags() -> RequestFlags {
        RequestFlags::new(
            false, false, false, false, false, false, false, false, false, false, false, None,
            None, None, None, None, None, None, None, None,
        )
    }

    fn push_to_vec(flags: &RequestFlags) -> Vec<u8> {
        let mut buf = Vec::new();
        flags.push_bytes(&mut buf, /* allow_no_reply_flag */ true);
        buf
    }

    #[test]
    fn test_empty_flags() {
        let flags = default_flags();
        assert_eq!(push_to_vec(&flags), b"");
    }

    #[test]
    fn test_no_reply() {
        let flags = RequestFlags::new(
            true, false, false, false, false, false, false, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" q");
    }

    #[test]
    fn test_return_client_flag() {
        let flags = RequestFlags::new(
            false, true, false, false, false, false, false, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" f");
    }

    #[test]
    fn test_return_cas_token() {
        let flags = RequestFlags::new(
            false, false, true, false, false, false, false, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" c");
    }

    #[test]
    fn test_return_value() {
        let flags = RequestFlags::new(
            false, false, false, true, false, false, false, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" v");
    }

    #[test]
    fn test_return_ttl() {
        let flags = RequestFlags::new(
            false, false, false, false, true, false, false, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" t");
    }

    #[test]
    fn test_return_size() {
        let flags = RequestFlags::new(
            false, false, false, false, false, true, false, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" s");
    }

    #[test]
    fn test_return_last_access() {
        let flags = RequestFlags::new(
            false, false, false, false, false, false, true, false, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" l");
    }

    #[test]
    fn test_return_fetched() {
        let flags = RequestFlags::new(
            false, false, false, false, false, false, false, true, false, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" h");
    }

    #[test]
    fn test_return_key() {
        let flags = RequestFlags::new(
            false, false, false, false, false, false, false, false, true, false, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" k");
    }

    #[test]
    fn test_no_update_lru() {
        let flags = RequestFlags::new(
            false, false, false, false, false, false, false, false, false, true, false, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" u");
    }

    #[test]
    fn test_mark_stale() {
        let flags = RequestFlags::new(
            false, false, false, false, false, false, false, false, false, false, true, None, None,
            None, None, None, None, None, None, None,
        );
        assert_eq!(push_to_vec(&flags), b" I");
    }

    #[test]
    fn test_cache_ttl() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            Some(300),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" T300");
    }

    #[test]
    fn test_recache_ttl() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            Some(60),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" R60");
    }

    #[test]
    fn test_vivify_on_miss_ttl() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            Some(120),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" N120");
    }

    #[test]
    fn test_client_flag() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            Some(42),
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" F42");
    }

    #[test]
    fn test_ma_initial_value() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            Some(100),
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" J100");
    }

    #[test]
    fn test_ma_delta_value() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            Some(5),
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" D5");
    }

    #[test]
    fn test_cas_token() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(999),
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" C999");
    }

    #[test]
    fn test_opaque() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(b"token123".to_vec()),
            None,
        );
        assert_eq!(push_to_vec(&flags), b" Otoken123");
    }

    // Mode optimization: SET_MODE_SET and MA_MODE_INC are defaults, not sent
    #[test]
    fn test_mode_set_not_sent() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(SET_MODE_SET),
        );
        assert_eq!(push_to_vec(&flags), b"");
    }

    #[test]
    fn test_mode_inc_not_sent() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(MA_MODE_INC),
        );
        assert_eq!(push_to_vec(&flags), b"");
    }

    #[test]
    fn test_mode_add_sent() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(SET_MODE_ADD),
        );
        assert_eq!(push_to_vec(&flags), b" ME");
    }

    #[test]
    fn test_mode_append_sent() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(SET_MODE_APPEND),
        );
        assert_eq!(push_to_vec(&flags), b" MA");
    }

    #[test]
    fn test_mode_dec_sent() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(MA_MODE_DEC),
        );
        assert_eq!(push_to_vec(&flags), b" M-");
    }

    #[test]
    fn test_large_u64_values() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            Some(u64::MAX),
            Some(u64::MAX),
            None,
            None,
            None,
        );
        let result = push_to_vec(&flags);
        let expected = format!(" J{} D{}", u64::MAX, u64::MAX);
        assert_eq!(result, expected.as_bytes());
    }

    #[test]
    fn test_zero_values() {
        let flags = RequestFlags::new(
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            None,
            None,
        );
        assert_eq!(push_to_vec(&flags), b" T0 R0 N0 F0 J0 D0 C0");
    }

    #[test]
    fn test_flag_ordering() {
        // Verify flags are emitted in the correct order
        let flags = RequestFlags::new(
            true,                 // q
            true,                 // f
            true,                 // c
            true,                 // v
            true,                 // t
            true,                 // s
            true,                 // l
            true,                 // h
            true,                 // k
            true,                 // u
            true,                 // I
            Some(1),              // T
            Some(2),              // R
            Some(3),              // N
            Some(4),              // F
            Some(5),              // J
            Some(6),              // D
            Some(7),              // C
            Some(b"op".to_vec()), // O
            Some(SET_MODE_ADD),   // M
        );
        assert_eq!(
            push_to_vec(&flags),
            b" q f c v t s l h k u I T1 R2 N3 F4 J5 D6 C7 Oop ME"
        );
    }

    // Helper: all-None replace call (no overrides)
    fn replace_none(flags: &RequestFlags) -> RequestFlags {
        flags.replace(
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None,
        )
    }

    #[test]
    fn test_replace_no_args_returns_equal() {
        let base = RequestFlags::new(
            true,
            true,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            Some(300),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(replace_none(&base), base);
    }

    #[test]
    fn test_replace_bool_flag() {
        let base = default_flags();
        let updated = base.replace(
            Some(true), // no_reply
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&updated), b" q");
        // base is unchanged
        assert_eq!(push_to_vec(&base), b"");
    }

    #[test]
    fn test_replace_optional_field() {
        let base = default_flags();
        let updated = base.replace(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(600), // cache_ttl
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&updated), b" T600");
        assert_eq!(push_to_vec(&base), b"");
    }

    #[test]
    fn test_replace_none_keeps_existing_optional() {
        // Passing None for an optional field keeps the existing value, not unsets it
        let base = RequestFlags::new(
            false, false, false, false, false, false, false, false, false, false, false,
            Some(300), // cache_ttl set
            None, None, None, None, None, None, None, None,
        );
        let updated = replace_none(&base);
        assert_eq!(push_to_vec(&updated), b" T300");
    }

    #[test]
    fn test_replace_multiple_fields() {
        let base = RequestFlags::new(
            false, true, false, true, false, false, false, false, false, false, false, Some(60),
            None, None, None, None, None, None, None, None,
        );
        let updated = base.replace(
            Some(true), // add no_reply
            None,       // keep return_client_flag=true
            Some(true), // add return_cas_token
            None,       // keep return_value=true
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,       // keep cache_ttl=60
            Some(120),  // add recache_ttl
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(push_to_vec(&updated), b" q f c v T60 R120");
    }

    #[test]
    fn test_replace_opaque() {
        let base = default_flags();
        let updated = base.replace(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(b"abc".to_vec()),
            None,
        );
        assert_eq!(push_to_vec(&updated), b" Oabc");
        // base is unchanged
        assert_eq!(push_to_vec(&base), b"");
    }
}
