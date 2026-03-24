use memchr::memmem;

use crate::constants::*;
use crate::response_flags::ResponseFlags;

/// Result of parsing a memcache meta-protocol response header.
#[derive(Debug)]
pub struct ParsedHeader {
    pub end_pos: usize,
    pub response_type: Option<u8>,
    pub size: Option<u32>,
    pub flags: Option<ResponseFlags>,
}

pub fn impl_parse_header(data: &[u8], start: usize, end: usize) -> Option<ParsedHeader> {
    if end - start < 4 {
        return None;
    }
    let end = end.min(data.len());
    let search_start = start + 2;
    if search_start >= end {
        return None;
    }
    // SIMD-accelerated search for \r\n
    let pos = memmem::find(&data[search_start..end], b"\r\n")?;
    let n = search_start + pos;
    let end_pos = n + 2;
    match &data[start..start + 2] {
        b"VA" => match ResponseFlags::from_value_header(&data[start..n]) {
            Some((size, flags)) => Some(ParsedHeader {
                end_pos,
                response_type: Some(RESPONSE_VALUE),
                size: Some(size),
                flags: Some(flags),
            }),
            None => Some(ParsedHeader {
                end_pos,
                response_type: None,
                size: None,
                flags: None,
            }),
        },
        b"HD" | b"OK" => Some(ParsedHeader {
            end_pos,
            response_type: Some(RESPONSE_SUCCESS),
            size: None,
            flags: Some(ResponseFlags::from_success_header(&data[start..n])),
        }),
        b"NS" => Some(ParsedHeader {
            end_pos,
            response_type: Some(RESPONSE_NOT_STORED),
            size: None,
            flags: None,
        }),
        b"EX" => Some(ParsedHeader {
            end_pos,
            response_type: Some(RESPONSE_CONFLICT),
            size: None,
            flags: None,
        }),
        b"EN" | b"NF" => Some(ParsedHeader {
            end_pos,
            response_type: Some(RESPONSE_MISS),
            size: None,
            flags: None,
        }),
        b"MN" => Some(ParsedHeader {
            end_pos,
            response_type: Some(RESPONSE_NOOP),
            size: None,
            flags: None,
        }),
        _ => Some(ParsedHeader {
            end_pos,
            response_type: None,
            size: None,
            flags: None,
        }),
    }
}
