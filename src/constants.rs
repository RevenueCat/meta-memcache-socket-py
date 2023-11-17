pub const RESPONSE_VALUE: u8 = 1; // VALUE (VA)
pub const RESPONSE_SUCCESS: u8 = 2; // SUCCESS (OK or HD)
pub const RESPONSE_NOT_STORED: u8 = 3; // NOT_STORED (NS)
pub const RESPONSE_CONFLICT: u8 = 4; // CONFLICT (EX)
pub const RESPONSE_MISS: u8 = 5; // MISS (EN or NF)
pub const RESPONSE_NOOP: u8 = 100; // NOOP (MN)

// Set modes:
// E: "add" command. LRU bump and return NS if item exists. Else add.
// A: "append" command. If item exists, append the new value to its data.
// P: "prepend" command. If item exists, prepend the new value to its data.
// R: "replace" command. Set only if item already exists.
// S: "set" command. The default mode, added for completeness.
pub const SET_MODE_ADD: u8 = 69; // 'E'
pub const SET_MODE_APPEND: u8 = 65; // 'A'
pub const SET_MODE_PREPEND: u8 = 80; // 'P'
pub const SET_MODE_REPLACE: u8 = 82; // 'R'
pub const SET_MODE_SET: u8 = 83; // 'S'
pub const MA_MODE_INC: u8 = 43; // '+'
pub const MA_MODE_DEC: u8 = 45; // '-'
