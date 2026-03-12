use crate::RequestFlags;
use crate::encode_key::encode_key;

pub struct BuiltCmd {
    pub buf: Vec<u8>,
    pub no_reply: bool,
}

pub fn impl_build_cmd(
    cmd: &[u8],
    key: &[u8],
    size: Option<u32>,
    request_flags: Option<&RequestFlags>,
    legacy_size_format: bool,
    allow_no_reply_flag: bool,
) -> Option<BuiltCmd> {
    let encoded_key = encode_key(key)?;

    // Build the command
    let mut buf: Vec<u8> = Vec::with_capacity(128);

    // Add CMD
    buf.extend_from_slice(cmd);
    buf.push(b' ');

    // Add wire-ready key (already base64-encoded if binary)
    buf.extend_from_slice(&encoded_key.value);

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
    if encoded_key.is_binary {
        // Tell the server to decode the base64 key and store it as binary
        buf.push(b' ');
        buf.push(b'b');
    }
    let no_reply = if let Some(request_flags) = request_flags {
        request_flags.push_bytes(&mut buf, allow_no_reply_flag);
        allow_no_reply_flag && request_flags.is_no_reply()
    } else {
        false
    };
    buf.push(b'\r');
    buf.push(b'\n');
    Some(BuiltCmd { buf, no_reply })
}
