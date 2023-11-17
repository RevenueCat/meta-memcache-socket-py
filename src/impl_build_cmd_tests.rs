#[cfg(test)]
mod tests {
    use crate::impl_build_cmd;
    use crate::request_flags::RequestFlags;

    #[test]
    fn test_impl_build_cmd_with_flags() {
        let cmd = b"mg";
        let key = b"key";
        let request_flags = RequestFlags::new(
            true,                     // no_reply
            true,                     // return_client_flag
            true,                     // return_cas_token
            true,                     // return_value
            true,                     // return_ttl
            true,                     // return_size
            true,                     // return_last_access
            true,                     // return_fetched
            true,                     // return_key
            true,                     // no_update_lru
            true,                     // mark_stale
            Some(111),                // cache_ttl
            Some(222),                // recache_ttl
            Some(333),                // vivify_on_miss_ttl
            Some(444),                // client_flag
            Some(555),                // ma_initial_value
            Some(666),                // ma_delta_value,
            Some(777),                // cas_token
            Some(b"opaque".to_vec()), // opaque
            Some(65 as u8),           // mode
        );

        let result = impl_build_cmd(cmd, key, None, Some(&request_flags), false).unwrap();
        let string = String::from_utf8_lossy(&result);
        println!("{:?}", string);
        assert_eq!(
            result,
            b"mg key q f c v t s l h k u I T111 R222 N333 F444 J555 D666 C777 Oopaque MA\r\n"
        );
    }

    #[test]
    fn test_impl_build_cmd_no_flags() {
        let cmd = b"mg";
        let key = b"key";
        let request_flags = RequestFlags::new(
            false, // no_reply
            false, // return_client_flag
            false, // return_cas_token
            false, // return_value
            false, // return_ttl
            false, // return_size
            false, // return_last_access
            false, // return_fetched
            false, // return_key
            false, // no_update_lru
            false, // mark_stale
            None,  // cache_ttl
            None,  // recache_ttl
            None,  // vivify_on_miss_ttl
            None,  // client_flag
            None,  // ma_initial_value
            None,  // ma_delta_value,
            None,  // cas_token
            None,  // opaque
            None,  // mode
        );

        let result = impl_build_cmd(cmd, key, None, Some(&request_flags), false).unwrap();
        let string = String::from_utf8_lossy(&result);
        println!("{:?}", string);
        assert_eq!(result, b"mg key\r\n");
    }

    #[test]
    fn test_impl_build_cmd_binary_key() {
        let cmd = b"mg";
        let key = b"Key_with_binary\x00";
        let request_flags = RequestFlags::new(
            false, // no_reply
            false, // return_client_flag
            false, // return_cas_token
            false, // return_value
            false, // return_ttl
            false, // return_size
            false, // return_last_access
            false, // return_fetched
            false, // return_key
            false, // no_update_lru
            false, // mark_stale
            None,  // cache_ttl
            None,  // recache_ttl
            None,  // vivify_on_miss_ttl
            None,  // client_flag
            None,  // ma_initial_value
            None,  // ma_delta_value,
            None,  // cas_token
            None,  // opaque
            None,  // mode
        );

        let result = impl_build_cmd(cmd, key, None, Some(&request_flags), false).unwrap();
        let string = String::from_utf8_lossy(&result);
        println!("{:?}", string);
        assert_eq!(result, b"mg S2V5X3dpdGhfYmluYXJ5AA== b\r\n");
    }

    #[test]
    fn test_impl_build_cmd_key_with_spaces() {
        let cmd = b"mg";
        let key = b"Key with spaces";
        let request_flags = RequestFlags::new(
            false, // no_reply
            false, // return_client_flag
            false, // return_cas_token
            false, // return_value
            false, // return_ttl
            false, // return_size
            false, // return_last_access
            false, // return_fetched
            false, // return_key
            false, // no_update_lru
            false, // mark_stale
            None,  // cache_ttl
            None,  // recache_ttl
            None,  // vivify_on_miss_ttl
            None,  // client_flag
            None,  // ma_initial_value
            None,  // ma_delta_value,
            None,  // cas_token
            None,  // opaque
            None,  // mode
        );

        let result = impl_build_cmd(cmd, key, None, Some(&request_flags), false).unwrap();
        let string = String::from_utf8_lossy(&result);
        println!("{:?}", string);
        assert_eq!(result, b"mg S2V5IHdpdGggc3BhY2Vz b\r\n");
    }

    #[test]
    fn test_impl_build_cmd_large_key() {
        let cmd = b"mg";
        let key = &vec![b'X'; 250];
        let no_result = impl_build_cmd(cmd, key, None, None, false);
        assert!(no_result.is_none());
    }

    #[test]
    fn test_cmd_with_size() {
        let cmd = b"ms";
        let key = b"key";
        let size = 123;
        let request_flags = RequestFlags::new(
            false,     // no_reply
            false,     // return_client_flag
            false,     // return_cas_token
            false,     // return_value
            false,     // return_ttl
            false,     // return_size
            false,     // return_last_access
            false,     // return_fetched
            false,     // return_key
            false,     // no_update_lru
            false,     // mark_stale
            Some(111), // cache_ttl
            None,      // recache_ttl
            None,      // vivify_on_miss_ttl
            None,      // client_flag
            None,      // ma_initial_value
            None,      // ma_delta_value,
            None,      // cas_token
            None,      // opaque
            None,      // mode
        );

        let result = impl_build_cmd(cmd, key, Some(size), Some(&request_flags), false).unwrap();
        let string = String::from_utf8_lossy(&result);
        println!("{:?}", string);
        assert_eq!(result, b"ms key 123 T111\r\n");
    }

    #[test]
    fn test_cmd_with_legacy_size() {
        let cmd = b"ms";
        let key = b"key";
        let size = 123;

        let result = impl_build_cmd(cmd, key, Some(size), None, true).unwrap();
        let string = String::from_utf8_lossy(&result);
        println!("{:?}", string);
        assert_eq!(result, b"ms key S123\r\n");
    }
}
