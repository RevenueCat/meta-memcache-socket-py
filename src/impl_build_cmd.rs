use base64::{engine::general_purpose, Engine as _};

use crate::RequestFlags;

const MAX_KEY_LEN: usize = 250;
const MAX_BIN_KEY_LEN: usize = 187; // 250 * 3 / 4 due to b64 encoding

// type KeyHasher = Blake2b<U18>;

pub fn impl_build_cmd(
    cmd: &[u8],
    key: &[u8],
    size: Option<u32>,
    request_flags: Option<&RequestFlags>,
    legacy_size_format: bool,
) -> Option<Vec<u8>> {
    if key.len() >= MAX_KEY_LEN {
        // Key is too long
        return None;
    }
    let mut binary = false;
    for c in key.iter() {
        if *c <= b' ' || *c > b'~' {
            // Not ascii or containing spaces
            binary = true;
            break;
        }
    }
    if binary && key.len() >= MAX_BIN_KEY_LEN {
        // Key is too long
        return None;
    }

    // Build the command
    let mut buf: Vec<u8> = Vec::new();

    // Add CMD
    buf.extend_from_slice(cmd);
    buf.push(b' ');

    // Add key
    if binary {
        // If the key contains binary or spaces, it will be send in b64
        let result = general_purpose::STANDARD.encode(key);
        buf.extend_from_slice(&result.as_bytes());
    } else {
        // Otherwise, it will be send as is
        buf.extend_from_slice(key);
    }

    // Add size
    if let Some(size) = size {
        buf.push(b' ');
        if legacy_size_format {
            buf.push(b'S');
        }
        buf.extend_from_slice(&size.to_string().as_bytes());
    }

    // Add request flags
    if binary {
        // If the key is binary, it will be send in b64. Adding the b flag will
        // tell the server to decode it and store it as binary, saving memory.
        buf.push(b' ');
        buf.push(b'b');
    }
    if let Some(request_flags) = request_flags {
        request_flags.push_bytes(&mut buf);
    }
    buf.push(b'\r');
    buf.push(b'\n');
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

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
