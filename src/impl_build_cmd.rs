use base64::{Engine as _, engine::general_purpose};

use crate::RequestFlags;

const MAX_KEY_LEN: usize = 250;
const MAX_BIN_KEY_LEN: usize = 187; // 250 * 3 / 4 due to b64 encoding

pub fn impl_build_cmd(
    cmd: &[u8],
    key: &[u8],
    size: Option<u32>,
    request_flags: Option<&RequestFlags>,
    legacy_size_format: bool,
) -> Option<Vec<u8>> {
    if key.is_empty() || key.len() >= MAX_KEY_LEN {
        return None;
    }
    let binary = key.iter().any(|&c| c <= b' ' || c > b'~');
    if binary && key.len() >= MAX_BIN_KEY_LEN {
        // Key is too long
        return None;
    }

    // Build the command
    let mut buf: Vec<u8> = Vec::with_capacity(128);

    // Add CMD
    buf.extend_from_slice(cmd);
    buf.push(b' ');

    // Add key
    if binary {
        // If the key contains binary or spaces, it will be send in b64
        let result = general_purpose::STANDARD.encode(key);
        buf.extend_from_slice(result.as_bytes());
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
        let mut itoa_buf = itoa::Buffer::new();
        buf.extend_from_slice(itoa_buf.format(size).as_bytes());
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
