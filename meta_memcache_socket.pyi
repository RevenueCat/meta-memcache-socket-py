import socket
from typing import Any, Final, Optional, Tuple, Union

RESPONSE_VALUE: int  # 1 - VALUE (VA)
RESPONSE_SUCCESS: int  # 2 - SUCCESS (OK or HD)
RESPONSE_NOT_STORED: int  # 3 - NOT_STORED (NS)
RESPONSE_CONFLICT: int  # 4 - CONFLICT (EX)
RESPONSE_MISS: int  # 5 - MISS (EN or NF)
RESPONSE_NOOP: int  # 100 - NOOP (MN)

# Set modes
# E "add" command. LRU bump and return NS if item exists. Else add.
SET_MODE_ADD: int  # 69 ('E')
# A "append" command. If item exists, append the new value to its data.
SET_MODE_APPEND: int  # 65 ('A')
# P "prepend" command. If item exists, prepend the new value to its data.
SET_MODE_PREPEND: int  # 80 ('P')
# R "replace" command. Set only if item already exists.
SET_MODE_REPLACE: int  # 82 ('R')
# S "set" command. The default mode, added for completeness.
SET_MODE_SET: int  # 83 ('S')
# Arithmetic modes
# + "increment"
MA_MODE_INC: int  # 43 ('+')
# - "decrement"
MA_MODE_DEC: int  # 45 ('-')

# Server versions
SERVER_VERSION_AWS_1_6_6: int  # 1
SERVER_VERSION_STABLE: int  # 2

class RequestFlags:
    """
    A class representing the flags for a meta-protocol request

    * no_reply: Set to True if the server should not send a response
    * return_client_flag: Set to True if the server should return the client flag
    * return_cas_token: Set to True if the server should return the CAS token
    * return_value: Set to True if the server should return the value
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

    no_reply: Final[bool]
    return_client_flag: Final[bool]
    return_cas_token: Final[bool]
    return_value: Final[bool]
    return_ttl: Final[bool]
    return_size: Final[bool]
    return_last_access: Final[bool]
    return_fetched: Final[bool]
    return_key: Final[bool]
    no_update_lru: Final[bool]
    mark_stale: Final[bool]
    cache_ttl: Final[Optional[int]]
    recache_ttl: Final[Optional[int]]
    vivify_on_miss_ttl: Final[Optional[int]]
    client_flag: Final[Optional[int]]
    ma_initial_value: Final[Optional[int]]
    ma_delta_value: Final[Optional[int]]
    cas_token: Final[Optional[int]]
    opaque: Final[Optional[bytes]]
    mode: Final[Optional[int]]

    def __init__(
        self,
        *,
        no_reply: bool = False,
        return_client_flag: bool = False,
        return_cas_token: bool = False,
        return_value: bool = False,
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
    def replace(
        self,
        *,
        no_reply: Optional[bool] = None,
        return_client_flag: Optional[bool] = None,
        return_cas_token: Optional[bool] = None,
        return_value: Optional[bool] = None,
        return_ttl: Optional[bool] = None,
        return_size: Optional[bool] = None,
        return_last_access: Optional[bool] = None,
        return_fetched: Optional[bool] = None,
        return_key: Optional[bool] = None,
        no_update_lru: Optional[bool] = None,
        mark_stale: Optional[bool] = None,
        cache_ttl: Optional[int] = None,
        recache_ttl: Optional[int] = None,
        vivify_on_miss_ttl: Optional[int] = None,
        client_flag: Optional[int] = None,
        ma_initial_value: Optional[int] = None,
        ma_delta_value: Optional[int] = None,
        cas_token: Optional[int] = None,
        opaque: Optional[bytes] = None,
        mode: Optional[int] = None,
    ) -> "RequestFlags": ...
    def to_bytes(self) -> bytes: ...
    def __str__(self) -> str: ...

class ResponseFlags:
    """
    A class representing the flags for a meta-protocol response

    * cas_token: Compare-And-Swap token (integer value) or None if not returned
    * fetched:
        - True if fetched since being set
        - False if not fetched since being set
        - None if the server did not return this flag info
    * last_access: time in seconds since last access (integer value) or None if not returned
    * ttl: time in seconds until the value expires (integer value) or None if not returned
        - The special value -1 represents if the key will never expire
    * client_flag: integer value or None if not returned
    * win:
        - True if the client won the right to repopulate
        - False if the client lost the right to repopulate
        - None if the server did not return a win/lose flag
    * stale: True if the value is stale, False otherwise
    * size: integer value or None if not returned
    * opaque: bytes value or None if not returned
    """

    cas_token: Optional[int]
    fetched: Optional[bool]
    last_access: Optional[int]
    ttl: Optional[int]
    client_flag: Optional[int]
    win: Optional[bool]
    stale: bool
    size: Optional[int]
    opaque: Optional[bytes]

    def __init__(
        self,
        *,
        cas_token: Optional[int] = None,
        fetched: Optional[bool] = None,
        last_access: Optional[int] = None,
        ttl: Optional[int] = None,
        client_flag: Optional[int] = None,
        win: Optional[bool] = None,
        stale: bool = False,
        size: Optional[int] = None,
        opaque: Optional[bytes] = None,
    ) -> None: ...
    def __str__(self) -> str: ...
    @staticmethod
    def from_success_header(header: bytes) -> "ResponseFlags":
        """Parse response flags from a success (HD) header."""
        ...

    @staticmethod
    def from_value_header(header: bytes) -> Optional[Tuple[int, "ResponseFlags"]]:
        """Parse size and response flags from a value (VA) header."""
        ...

    @staticmethod
    def parse_flags(header: bytes, start: int) -> "ResponseFlags":
        """Parse response flags from a header starting at the given position."""
        ...

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
    key: Union[str, bytes],
    size: Optional[int] = None,
    request_flags: Optional[RequestFlags] = None,
    legacy_size_format: bool = False,
) -> bytes:
    """
    Build a memcache meta-protocol command

    :param cmd: The command to send
    :param key: The key to use
    :param size: The size of the value (for set commands)
    :param request_flags: The flags to use
    :param legacy_size_format: Whether to use legacy size syntax from 1.6.6
    """
    ...

def build_meta_get(
    key: Union[str, bytes],
    request_flags: Optional[RequestFlags] = None,
) -> bytes:
    """
    Build a memcache meta-get command

    :param key: The key to use
    :param request_flags: The flags to use
    """
    ...

def build_meta_delete(
    key: Union[str, bytes],
    request_flags: Optional[RequestFlags] = None,
) -> bytes:
    """
    Build a memcache meta-delete command

    :param key: The key to use
    :param request_flags: The flags to use
    """
    ...

def build_meta_set(
    key: Union[str, bytes],
    size: int,
    request_flags: Optional[RequestFlags] = None,
    legacy_size_format: bool = False,
) -> bytes:
    """
    Build a memcache meta-set command

    :param key: The key to use
    :param size: The size of the value
    :param request_flags: The flags to use
    :param legacy_size_format: Whether to use legacy size syntax from 1.6.6
    """
    ...

def build_meta_arithmetic(
    key: Union[str, bytes],
    request_flags: Optional[RequestFlags] = None,
) -> bytes:
    """
    Build a memcache meta-arithmetic command

    :param key: The key to use
    :param request_flags: The flags to use
    """
    ...

class Miss:
    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...
    def __bool__(self) -> bool: ...

class NotStored:
    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...
    def __bool__(self) -> bool: ...

class Conflict:
    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...
    def __bool__(self) -> bool: ...

class Success:
    flags: ResponseFlags

    def __init__(self, flags: ResponseFlags) -> None: ...
    def __repr__(self) -> str: ...

class Value(Success):
    size: int
    value: Any

    def __init__(
        self,
        size: int,
        flags: ResponseFlags,
        value: Any = None,
    ) -> None: ...
    def __repr__(self) -> str: ...

class MemcacheSocket:
    """
    A high-performance memcache socket that handles the meta-protocol
    communication with a memcached server.

    Releases the GIL during socket I/O operations.
    """

    def __init__(
        self,
        conn: socket.socket,
        buffer_size: int = 4096,
        version: int = ...,  # SERVER_VERSION_STABLE
    ) -> None: ...
    def __str__(self) -> str: ...
    def get_version(self) -> int: ...
    def set_socket(self, conn: socket.socket) -> None: ...
    def close(self) -> None: ...
    def sendall(self, data: bytes, with_noop: bool) -> None: ...
    def get_response(self) -> Union[Value, Success, Miss, NotStored, Conflict]: ...
    # send_meta_* methods (for pipelining — send only, read later with get_response())
    # Mutations automatically inject NOOP when no_reply is set in request_flags.
    def send_meta_get(
        self,
        key: Union[str, bytes],
        request_flags: Optional[RequestFlags] = None,
    ) -> None: ...
    def send_meta_set(
        self,
        key: Union[str, bytes],
        value: bytes,
        request_flags: Optional[RequestFlags] = None,
    ) -> None: ...
    def send_meta_delete(
        self,
        key: Union[str, bytes],
        request_flags: Optional[RequestFlags] = None,
    ) -> None: ...
    def send_meta_arithmetic(
        self,
        key: Union[str, bytes],
        request_flags: Optional[RequestFlags] = None,
    ) -> None: ...

    # meta_* methods (blocking — send + recv in one call)
    def meta_get(
        self,
        key: Union[str, bytes],
        request_flags: Optional[RequestFlags] = None,
    ) -> Union[Value, Success, Miss, NotStored, Conflict]: ...
    def meta_set(
        self,
        key: Union[str, bytes],
        value: bytes,
        request_flags: Optional[RequestFlags] = None,
    ) -> Union[Value, Success, Miss, NotStored, Conflict]: ...
    def meta_delete(
        self,
        key: Union[str, bytes],
        request_flags: Optional[RequestFlags] = None,
    ) -> Union[Value, Success, Miss, NotStored, Conflict]: ...
    def meta_arithmetic(
        self,
        key: Union[str, bytes],
        request_flags: Optional[RequestFlags] = None,
    ) -> Union[Value, Success, Miss, NotStored, Conflict]: ...

    # Batch operations
    def meta_multiget(
        self,
        keys: list[Union[str, bytes]],
        request_flags: Optional[RequestFlags] = None,
    ) -> list[Union[Value, Success, Miss, NotStored, Conflict]]:
        """
        Send multiple meta get commands and return all responses in one batch.

        Builds all commands into one buffer, sends in a single operation, then
        receives all responses in a tight Rust loop. GIL is released during
        all socket I/O. Returns a list of responses in the same order as keys.

        :param keys: List of keys to get
        :param request_flags: The flags to use for all keys
        """
        ...
