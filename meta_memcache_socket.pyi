from typing import Optional, Tuple, Union

RESPONSE_VALUE = 1  # VALUE (VA)
RESPONSE_SUCCESS = 2  # SUCCESS (OK or HD)
RESPONSE_NOT_STORED = 3  # NOT_STORED (NS)
RESPONSE_CONFLICT = 4  # CONFLICT (EX)
RESPONSE_MISS = 5  # MISS (EN or NF)
RESPONSE_NOOP = 100  # NOOP (MN)

# Set modes
# E "add" command. LRU bump and return NS if item exists. Else add.
SET_MODE_ADD = 69
# A "append" command. If item exists, append the new value to its data.
SET_MODE_APPEND = 65  # 'A'
# P "prepend" command. If item exists, prepend the new value to its data.
SET_MODE_PREPEND = 80  # 'P'
# R "replace" command. Set only if item already exists.
SET_MODE_REPLACE = 82  # 'R'
# S "set" command. The default mode, added for completeness.
SET_MODE_SET = 83  # 'S'
# Arithmetic modes
# + "increment"
MA_MODE_INC = 43
# - "decrement"
MA_MODE_DEC = 45

class RequestFlags:
    """
    A class representing the flags for a meta-protocol request

    * no_reply: Set to True if the server should not send a response
    * return_client_flag: Set to True if the server should return the client flag
    * return_cas_token: Set to True if the server should return the CAS token
    * return_value: Set to True if the server should return the value (Default)
    * return_ttl: Set to True if the server should return the TTL
    * return_size: Set to True if the server should return the size (useful
        if when paired with return_value=False, to get the size of the value)
    * return_last_access: Set to True if the server should return the last access time
    * return_fetched: Set to True if the server should return the fetched flag
    * return_key: Set to True if the server should return the key in the response
    * no_update_lru: Set to True if the server should not update the LRU on this access
    * mark_stale: Set to True if the server should mark the value as stale
    * cache_ttl: The TTL to set on the key
    * recache_ttl: The TTL to use for recache policy
    * vivify_on_miss_ttl: The TTL to use when vivifying a value on a miss
    * client_flag: The client flag to store along the value (Useful to store value type, compression, etc)
    * ma_initial_value: For arithmetic operations, the initial value to use (if the key does not exist)
    * ma_delta_value: For arithmetic operations, the delta value to use
    * cas_token: The CAS token to use when storing the value in the cache
    * opaque: The opaque flag (will be echoed back in the response)
    * mode: The mode to use when storing the value in the cache. See SET_MODE_* and MA_MODE_* constants
    """

    no_reply: bool
    return_client_flag: bool
    return_cas_token: bool
    return_value: bool
    return_ttl: bool
    return_size: bool
    return_last_access: bool
    return_fetched: bool
    return_key: bool
    no_update_lru: bool
    mark_stale: bool
    cache_ttl: Optional[int]
    recache_ttl: Optional[int]
    vivify_on_miss_ttl: Optional[int]
    client_flag: Optional[int]
    ma_initial_value: Optional[int]
    ma_delta_value: Optional[int]
    cas_token: Optional[int]
    opaque: Optional[bytes]
    mode: Optional[int]

    def __init__(
        *,
        no_reply: bool = False,
        return_client_flag: bool = True,
        return_cas_token: bool = False,
        return_value=True,
        return_ttl: bool = False,
        return_size: bool = False,
        return_last_access: bool = False,
        return_fetched: bool = False,
        return_key: bool = False,
        no_update_lru: bool = False,
        mark_stale: bool = False,
        cache_ttl: Optional[int] = None,
        recache_ttl: Optional[int] = None,
        vivify_on_miss_ttl: Optional[int] = None,
        client_flag: Optional[int] = None,
        ma_initial_value: Optional[int] = None,
        ma_delta_value: Optional[int] = None,
        cas_token: Optional[int] = None,
        opaque: Optional[bytes] = None,
        mode: Optional[int] = None,
    ) -> None: ...
    def copy(self) -> "RequestFlags": ...
    def to_bytes(self) -> bytes: ...

class ResponseFlags:
    """
    A class representing the flags for a meta-protocol response

    * cas_token: Compare-And-Swap token (integer value) or None if not returned
    * fetched:
        - True if fetched since being set
        - False if not fetched since being set
        - None if the server di not return this flag info
    * last_access: time in seconds since last access (integer value) or None if not returned
    * ttl: time in seconds until the value expires (integer value) or None if not returned
        - The special value -1 represents if the key will never expire
    * client_flag: integer value or None if not returned
    * win:
        - True if the client won the right to repopulate
        - False if the client lost the right to repopulate
        - None if the server did not return a win/lose flag
    * stale: True if the value is stale, False otherwise
    * real_size: integer value or None if not returned
    * opaque flag: bytes value or None if not returned
    """

    cas_token: Optional[int]
    fetched: Optional[bool]
    last_access: Optional[int]
    ttl: Optional[int]
    client_flag: Optional[int]
    win: Optional[bool]
    stale: bool
    real_size: Optional[int]
    opaque: Optional[bytes]

    def __init__(
        *,
        cas_token=None,
        fetched=None,
        last_access=None,
        ttl=None,
        client_flag=None,
        win=None,
        stale=False,
        size=None,
        opaque=None,
    ) -> None: ...

def parse_header(
    buffer: Union[memoryview, bytes, bytearray],
    start: int,
    end: int,
) -> Optional[Tuple[int, Optional[int], Optional[int], Optional[ResponseFlags]]]:
    """
    Parse a memcache meta-protocol header from a buffer

    :param buffer: The buffer to parse
    :param start: The starting point in the buffer
    :param end: The end of the data read into the buffer
    """
    ...

def build_cmd(
    cmd: bytes,
    key: bytes,
    size: Optional[int] = None,
    flags: Optional[RequestFlags] = None,
    legacy_size_format: bool = False,
) -> bytes:
    """
    Build a memcache meta-protocol command

    :param cmd: The command to send
    :param key: The key to use
    :param flags: The flags to use
    :param legacy_size_format: Wether to legacy size syntax from 1.6.6
    """
    ...

def build_meta_get(
    key: bytes,
    flags: Optional[RequestFlags] = None,
) -> bytes:
    """
    Build a memcache meta-get command

    :param key: The key to use
    :param flags: The flags to use
    """
    ...

def build_meta_delete(
    key: bytes,
    flags: Optional[RequestFlags] = None,
) -> bytes:
    """
    Build a memcache meta-get command

    :param key: The key to use
    :param flags: The flags to use
    """
    ...

def build_meta_set(
    key: bytes,
    size: Optional[int] = None,
    flags: Optional[RequestFlags] = None,
    legacy_size_format: bool = False,
) -> bytes:
    """
    Build a memcache meta-set command

    :param key: The key to use
    :param flags: The flags to use
    """
    ...

def build_meta_arithmetic(
    key: bytes,
    flags: Optional[RequestFlags] = None,
) -> bytes:
    """
    Build a memcache meta-arithmetic command

    :param key: The key to use
    :param flags: The flags to use
    """
    ...
