# meta-memcache-socket

A high-performance Rust extension for Python that provides socket I/O, command
building, and response parsing for the
[Memcache meta-protocol](https://github.com/memcached/memcached/wiki/MetaCommands).
Designed as the low-level transport layer for
[meta-memcache-py](https://github.com/RevenueCat/meta-memcache-py).

## Key features

- **Rust-native socket I/O** — direct `send()`/`recv()`/`poll()` syscalls,
  bypassing Python's socket layer while still respecting `settimeout()`
- **GIL-free** — releases the GIL during all socket operations (`py.detach()`),
  so other Python threads run freely while waiting on the network
- **Zero-copy where possible** — response values are read directly into the
  internal buffer; `PyBytes` is created from the buffer slice without
  intermediate allocation
- **SIMD-accelerated parsing** — uses `memchr` for fast `\r\n` scanning
- **Free-threaded Python support** — built with `gil_used = false`, compatible
  with Python 3.13t (no-GIL builds)

## Project structure

```
meta-memcache-socket-py/
├── Cargo.toml                      # Rust package manifest
├── pyproject.toml                  # Python package manifest (maturin backend)
├── src/
│   ├── lib.rs                      # PyO3 module entry — exports classes, functions, constants
│   ├── constants.rs                # Protocol constants (response codes, set modes, NOOP, ENDL)
│   ├── memcache_socket.rs          # MemcacheSocket class — socket I/O, buffering, GIL management
│   ├── request_flags.rs            # RequestFlags class — immutable flags for building commands
│   ├── response_flags.rs           # ResponseFlags class — immutable flags parsed from responses
│   ├── response_types.rs           # Response type classes (Value, Success, Miss, NotStored, Conflict)
│   ├── impl_build_cmd.rs           # Command builder — key validation, base64, flag encoding
│   ├── impl_parse_header.rs        # Header parser — SIMD search, flag parsing, atoi
│   ├── impl_build_cmd_tests.rs     # Rust unit tests for command building
│   ├── impl_parse_header_tests.rs  # Rust unit tests for header parsing
│   ├── request_flags_tests.rs      # Rust unit tests for RequestFlags
│   └── response_flags_tests.rs     # Rust unit tests for ResponseFlags
├── tests/
│   ├── test_memcache_socket.py     # Python tests — socket I/O, timeouts, buffering, NOOP
│   └── test_response_types.py      # Python tests — response type semantics
├── bench.py                        # Microbenchmarks for command building and header parsing
└── .github/workflows/CI.yml        # CI — Rust tests, Python tests, cross-platform wheel builds
```

## Design overview

### Architecture

The module is a single Rust cdylib compiled with [PyO3](https://pyo3.rs/) and
packaged with [Maturin](https://www.maturin.rs/). There is no Python source
code — everything is implemented in Rust and exported to Python directly.

The design separates into three layers:

1. **Protocol layer** (`constants.rs`, `impl_build_cmd.rs`,
   `impl_parse_header.rs`) — stateless functions that build command byte strings
   and parse response headers. These know the meta-protocol grammar but nothing
   about sockets.

2. **Type layer** (`request_flags.rs`, `response_flags.rs`,
   `response_types.rs`) — Python-visible classes that carry request parameters
   and parsed response data.

3. **I/O layer** (`memcache_socket.rs`) — the `MemcacheSocket` class that owns
   a raw file descriptor, an internal read buffer, and a NOOP counter. All
   socket operations release the GIL via `py.detach()` and use `poll()` to
   handle non-blocking sockets with proper timeout support.

### MemcacheSocket internals

```
┌──────────────────────────────────────────────────────────┐
│ MemcacheSocket (Python-visible)                          │
│  • _conn: Py<PyAny>   — prevents Python GC of socket    │
│  • version: u8         — server version for compat       │
│  • io: SocketIO        — all I/O state (Send + Ungil)    │
│    ├── fd: RawFd                                         │
│    ├── buf: Vec<u8>    — ring buffer for recv'd data     │
│    ├── pos / read      — read cursor / write cursor      │
│    ├── timeout_ms      — poll() timeout from settimeout  │
│    └── noop_expected   — pending NOOP responses to drain  │
└──────────────────────────────────────────────────────────┘
```

The `SocketIO` struct contains no Python objects, so it satisfies PyO3's
`Ungil` trait and can be passed to `py.detach()` closures that release the GIL.

**Buffer management**: the internal buffer acts as a sliding window. When `pos`
passes 75% of the buffer, remaining data is shifted to the front
(`copy_within`). Values that fit in the buffer are served directly from it
(zero-copy to Rust); values exceeding the buffer are allocated into a temporary
`Vec`.

**NOOP handling**: when `sendall()` is called with `with_noop=True`, a `mn\r\n`
command is appended. The NOOP counter increments. On the next `get_response()`,
all responses before the corresponding `MN` are drained automatically, enabling
pipelined fire-and-forget commands.

**Timeout handling**: at construction time (and on `set_socket()`), the Python
socket's `gettimeout()` is read and converted to milliseconds for `poll()`. If
the socket is blocking (`gettimeout()` returns `None`), poll uses `-1`
(infinite). If a timeout is set, poll respects it and raises Python's
`TimeoutError` on expiry.

## API reference

### MemcacheSocket

The main class for socket communication with a Memcache server.

```python
from meta_memcache_socket import MemcacheSocket

# Constructor
ms = MemcacheSocket(
    conn,                        # Python socket object
    buffer_size=4096,            # Internal read buffer size in bytes
    version=SERVER_VERSION_STABLE,  # Server version for protocol compat
)

# Send data, optionally appending a NOOP command
ms.sendall(data: bytes, with_noop: bool)

# Read and parse the next response header
# Returns one of: Value, Success, Miss, NotStored, Conflict
resp = ms.get_response()

# Read value payload (call after get_response() returns a Value)
data: bytes = ms.get_value(resp.size)

# Replace the underlying socket (e.g. after reconnect)
ms.set_socket(new_conn)

# Close the underlying socket
ms.close()

# Server version
ms.get_version()  # -> int
```

### Response types

All response types are returned by `get_response()`:

| Class | Protocol code | Bool | Fields |
|---|---|---|---|
| `Miss` | `EN`, `NF` | `False` | — |
| `NotStored` | `NS` | `False` | — |
| `Conflict` | `EX` | `False` | — |
| `Success` | `HD`, `OK` | `True` | `flags: ResponseFlags` |
| `Value` | `VA` | `True` | `size: int`, `flags: ResponseFlags`, `value: Any` (settable) |

`Miss`, `NotStored`, and `Conflict` are frozen and support equality.
`Value.value` is a mutable slot used by higher-level code (e.g. meta-memcache-py's
executor) to attach deserialized data.

### ResponseFlags

Immutable (frozen) container for flags parsed from a server response.

```python
flags.cas_token     # Optional[int] — CAS token (c)
flags.fetched       # Optional[bool] — fetched from cache (h)
flags.last_access   # Optional[int] — seconds since last access (l)
flags.ttl           # Optional[int] — TTL in seconds, -1 = no expiry (t)
flags.client_flag   # Optional[int] — user-defined flag (f)
flags.win           # Optional[bool] — True=W (won), False=Z (lost)
flags.stale         # bool — marked stale (X)
flags.size          # Optional[int] — value size (s)
flags.opaque        # Optional[bytes] — echoed opaque data (O)
```

### RequestFlags

Immutable container for flags sent with commands.

```python
from meta_memcache_socket import RequestFlags

flags = RequestFlags(
    # Boolean flags
    no_reply=False,           # q — don't expect a response
    return_client_flag=True,  # f
    return_cas_token=True,    # c
    return_value=True,        # v
    return_ttl=False,         # t
    return_size=False,        # s
    return_last_access=False, # l
    return_fetched=False,     # h
    return_key=False,         # k
    no_update_lru=False,      # u
    mark_stale=False,         # I

    # Optional value flags
    cache_ttl=3600,           # T — TTL in seconds
    recache_ttl=None,         # R — recache window
    vivify_on_miss_ttl=None,  # N — create-on-miss TTL
    client_flag=42,           # F — user-defined flag
    ma_initial_value=None,    # J — arithmetic initial value
    ma_delta_value=None,      # D — arithmetic delta
    cas_token=None,           # C — CAS token for conditional ops
    opaque=None,              # O — opaque data echoed back
    mode=None,                # M — operation mode (set/arithmetic)
)
```

The flags are immutable, so they can be reused safely across threads when
calling meta commands. Internal layers migth need to mutate flags
(content id, reduce ttl, etc...) and will mutate them use replace() to create
modified copies when needed.

If you need to change flags on a existing RequestFlags, use the `replace()` method:

```python
new_flags = flags.replace(return_ttl=True, cache_ttl=600)  # -> RequestFlags
```

You can also encode the flags into a byte string for command building, showing
exactly what will be sent on the wire:

```python
flags.to_bytes()   # -> bytes (encoded flag string)
```

For debugging purposes, stringifying it shows the flags in a human-readable format.

### Command builders

Convenience functions that build meta-protocol command byte strings.
All raise `ValueError` if the key exceeds the length limit (250 bytes, or
187 for binary keys which are base64-encoded with a `b` flag).

```python
from meta_memcache_socket import (
    build_meta_get,
    build_meta_set,
    build_meta_delete,
    build_meta_arithmetic,
    build_cmd,
)

# mg key [flags]\r\n
cmd = build_meta_get(key: bytes, request_flags=None)

# ms key size [flags]\r\n
cmd = build_meta_set(key: bytes, size: int, request_flags=None, legacy_size_format=False)

# md key [flags]\r\n
cmd = build_meta_delete(key: bytes, request_flags=None)

# ma key [flags]\r\n
cmd = build_meta_arithmetic(key: bytes, request_flags=None)

# Generic: {cmd} key [size] [flags]\r\n
cmd = build_cmd(cmd: bytes, key: bytes, size=None, request_flags=None, legacy_size_format=False)
```

### parse_header

Low-level function to parse a response header from a buffer. Primarily used
internally by `MemcacheSocket.get_response()`, but exposed for advanced use.

```python
from meta_memcache_socket import parse_header

# Returns (end_pos, response_type, size, flags) or None if header is incomplete
result = parse_header(
    buffer: Union[memoryview, bytearray],
    start: int,
    end: int,
)
```

### Constants

```python
# Response type codes
RESPONSE_VALUE = 1
RESPONSE_SUCCESS = 2
RESPONSE_NOT_STORED = 3
RESPONSE_CONFLICT = 4
RESPONSE_MISS = 5
RESPONSE_NOOP = 100

# Set modes (for RequestFlags.mode)
SET_MODE_SET = 83       # 'S' — default set
SET_MODE_ADD = 69       # 'E' — add (only if not exists)
SET_MODE_REPLACE = 82   # 'R' — replace (only if exists)
SET_MODE_APPEND = 65    # 'A' — append to value
SET_MODE_PREPEND = 80   # 'P' — prepend to value

# Arithmetic modes
MA_MODE_INC = 43        # '+' — increment
MA_MODE_DEC = 45        # '-' — decrement

# Server versions
SERVER_VERSION_AWS_1_6_6 = 1   # AWS ElastiCache 1.6.6 compat
SERVER_VERSION_STABLE = 2      # Standard memcached
```

## Development

### Prerequisites

- [Rust](https://rustup.rs/) (stable toolchain, edition 2024)
- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) (recommended) or pip + maturin

### Building

```bash
# Build and install into the project venv (development mode)
uv run --with maturin maturin develop

# Build in release mode (optimized)
uv run --with maturin maturin develop --release
```

### Running tests

**Rust unit tests** — tests command building, header parsing, and flag encoding:

```bash
cargo test
```

**Python integration tests** — tests socket I/O, timeouts, buffering, response
types, and NOOP handling using real socket pairs:

```bash
# Build the extension, then run pytest
uv run --with maturin maturin develop
uv run --with pytest pytest tests/ -v
```

### Running benchmarks

```bash
uv run --with maturin maturin develop --release
uv run python bench.py
```

## Using a local build with meta-memcache-py

When developing this package alongside
[meta-memcache-py](https://github.com/RevenueCat/meta-memcache-py), you need
meta-memcache-py to use your local build instead of the PyPI version.

### Option 1: pip install from local path (quick iteration)

```bash
cd /path/to/meta-memcache-py

# Install the local build (--reinstall forces replacement of the existing version)
uv pip install -n -v /path/to/meta-memcache-socket-py --reinstall
```

NOTE: When using this option, any `uv run` will revert the package to the
version specified in the pyproject.toml file.

### Option 2: pyproject.toml dependency override (persistent)

In `meta-memcache-py`'s `pyproject.toml`, replace the PyPI dependency with a
local file reference:

```toml
dependencies = [
    # "meta-memcache-socket>=2.0.0",  # PyPI version (commented out)
    "meta-memcache-socket @ file:///path/to/meta-memcache-socket-py",
]
```

Then sync the environment:

```bash
uv sync
```

Remember to revert this before committing.

## Releasing

Releases are automated via GitHub Actions CI.

### Process

1. Update the version in `Cargo.toml`:
   ```toml
   [package]
   version = "2.1.0"
   ```

2. Commit and push to `main`.

3. Create and push a git tag:
   ```bash
   git tag v2.1.0
   git push origin v2.1.0
   ```

4. The CI pipeline will:
   - Run Rust and Python tests
   - Build wheels for all platforms:
     - Linux: x86_64, x86, aarch64, armv7, s390x, ppc64le (glibc + musl)
     - macOS: x86_64 (Intel), aarch64 (Apple Silicon)
     - Windows: x64, x86
   - Build for both CPython 3.x and free-threaded 3.13t
   - Generate build provenance attestation
   - Publish all wheels + sdist to PyPI

The PyPI upload uses the `PYPI_API_TOKEN` repository secret.

### Manual trigger

The release job can also be triggered manually via GitHub's "Run workflow"
button on the CI workflow page (`workflow_dispatch`). This runs all build jobs
and generates artifacts but only publishes to PyPI if a tag is present.

## Dependencies

| Crate | Purpose |
|---|---|
| [pyo3](https://pyo3.rs/) 0.28 | Python ↔ Rust bindings, GIL management |
| [libc](https://docs.rs/libc) | Direct syscalls: `poll`, `send`, `recv`, `writev`, `setsockopt` |
| [memchr](https://docs.rs/memchr) | SIMD-accelerated `\r\n` scanning |
| [atoi](https://docs.rs/atoi) | Fast ASCII → integer for header parsing |
| [itoa](https://docs.rs/itoa) | Fast integer → ASCII for command building |
| [base64](https://docs.rs/base64) | Binary key encoding |
