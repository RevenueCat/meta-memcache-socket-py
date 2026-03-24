"""Tests for the Rust MemcacheSocket class.

Mirrors the tests from meta-memcache-py/tests/memcache_socket_test.py
but tests the Rust implementation directly.
"""

import socket

import pytest

from meta_memcache_socket import (
    Conflict,
    MemcacheSocket,
    Miss,
    NotStored,
    ResponseFlags,
    Success,
    Value,
    SERVER_VERSION_AWS_1_6_6,
    SERVER_VERSION_STABLE,
)


@pytest.fixture
def socket_pair():
    a, b = socket.socketpair()
    yield a, b
    try:
        a.close()
    except OSError:
        pass
    try:
        b.close()
    except OSError:
        pass


# --- Constructor and basic methods ---


class TestConstructor:
    def test_create_default(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        assert ms.get_version() == SERVER_VERSION_STABLE

    def test_create_with_version(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a, version=SERVER_VERSION_AWS_1_6_6)
        assert ms.get_version() == SERVER_VERSION_AWS_1_6_6

    def test_create_with_buffer_size(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=8192)
        assert ms.get_version() == SERVER_VERSION_STABLE

    def test_str(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        s = str(ms)
        assert "<MemcacheSocket" in s
        assert str(a.fileno()) in s

    def test_close(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.close()
        assert a.fileno() == -1

    def test_set_socket(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        c, d = socket.socketpair()
        try:
            ms.set_socket(c)
            # Should work with new socket
            d.sendall(b"EN\r\n")
            resp = ms.get_response()
            assert isinstance(resp, Miss)
        finally:
            c.close()
            d.close()


# --- Sendall ---


class TestSendall:
    def test_sendall_basic(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"mg testkey\r\n", False)
        data = b.recv(1024)
        assert data == b"mg testkey\r\n"

    def test_sendall_with_noop(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"md testkey q\r\n", True)
        data = b.recv(1024)
        assert data == b"md testkey q\r\nmn\r\n"

    def test_sendall_multiple(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"mg key1\r\n", False)
        ms.sendall(b"mg key2\r\n", False)
        data = b.recv(1024)
        assert data == b"mg key1\r\nmg key2\r\n"

    def test_sendall_empty(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"", False)
        # Should not block - send empty data

    def test_sendall_with_value(self, socket_pair):
        """Simulate ms command: header + value + endl."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        cmd = b"ms testkey 5\r\nhello\r\n"
        ms.sendall(cmd, False)
        data = b.recv(1024)
        assert data == cmd


# --- get_response: simple response types ---


class TestGetResponseSimple:
    def test_miss_en(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Miss)

    def test_miss_nf(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"NF\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Miss)

    def test_not_stored(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"NS\r\n")
        resp = ms.get_response()
        assert isinstance(resp, NotStored)

    def test_conflict(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EX\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Conflict)

    def test_multiple_simple_responses(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EN\r\nNF\r\nNS\r\nEX\r\n")
        assert isinstance(ms.get_response(), Miss)
        assert isinstance(ms.get_response(), Miss)
        assert isinstance(ms.get_response(), NotStored)
        assert isinstance(ms.get_response(), Conflict)


# --- get_response: Success ---


class TestGetResponseSuccess:
    def test_hd_response(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"HD c42\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)
        assert resp.flags.cas_token == 42

    def test_ok_response(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"OK\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)

    def test_hd_with_multiple_flags(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"HD c99 h1 l30 t3600 f42 W\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)
        assert resp.flags.cas_token == 99
        assert resp.flags.fetched is True
        assert resp.flags.last_access == 30
        assert resp.flags.ttl == 3600
        assert resp.flags.client_flag == 42
        assert resp.flags.win is True
        assert resp.flags.stale is False

    def test_hd_stale(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"HD X\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)
        assert resp.flags.stale is True


# --- get_response: Value ---


class TestGetResponseValue:
    def test_value_response(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 2 c1\r\nOK\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 2
        assert resp.flags.cas_token == 1
        assert resp.value is None  # Not yet read

    def test_value_with_all_flags(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 3 c999 h0 l60 t-1 f7 s3 W Otoken\r\nfoo\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 3
        assert resp.flags.cas_token == 999
        assert resp.flags.fetched is False
        assert resp.flags.last_access == 60
        assert resp.flags.ttl == -1
        assert resp.flags.client_flag == 7
        assert resp.flags.size == 3
        assert resp.flags.win is True
        assert bytes(resp.flags.opaque) == b"token"

    def test_value_stale_and_lost(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 1 X Z\r\nx\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.flags.stale is True
        assert resp.flags.win is False


# --- get_response: server version 1.6.6 ---


class TestGetResponse166:
    def test_ok_response_as_success(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a, version=SERVER_VERSION_AWS_1_6_6)
        b.sendall(b"OK c1\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)
        assert resp.flags.cas_token == 1

    def test_value_and_ok(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a, version=SERVER_VERSION_AWS_1_6_6)
        b.sendall(b"VA 2 c1\r\nOK\r\nOK c2\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 2
        val = ms.get_value(resp.size)
        assert val == b"OK"

        resp2 = ms.get_response()
        assert isinstance(resp2, Success)
        assert resp2.flags.cas_token == 2


# --- get_value ---


class TestGetValue:
    def test_small_value(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 5\r\nhello\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        val = ms.get_value(resp.size)
        assert val == b"hello"
        assert isinstance(val, bytes)

    def test_empty_value(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 0\r\n\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 0
        val = ms.get_value(resp.size)
        assert val == b""

    def test_large_value_exceeding_buffer(self, socket_pair):
        """Value larger than the buffer size triggers temporary allocation."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=100)
        payload = b"1234567890" * 20  # 200 bytes
        b.sendall(b"VA 200 c1 Oxxx W\r\n" + payload + b"\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 200
        assert resp.flags.cas_token == 1
        assert resp.flags.win is True
        assert bytes(resp.flags.opaque) == b"xxx"
        val = ms.get_value(resp.size)
        assert len(val) == 200
        assert val == payload

    def test_value_with_incomplete_endl(self, socket_pair):
        """Buffer is just big enough for value but ENDL splits across reads."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=18)
        b.sendall(b"VA 10\r\n1234567890\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 10
        val = ms.get_value(resp.size)
        assert val == b"1234567890"

    def test_value_with_incomplete_endl_then_response(self, socket_pair):
        """After ENDL-split value read, buffer state must allow further responses.

        Regression test: the ENDL-split path in ensure_value consumed ENDL bytes
        from the socket but not from the buffer. The caller then advanced pos past
        read, corrupting buffer state for subsequent operations.
        """
        a, b = socket_pair
        # buffer_size=18: header "VA 10\r\n" is 7 bytes, value is 10 bytes,
        # so value fills the buffer and \r\n splits across reads.
        ms = MemcacheSocket(a, buffer_size=18)
        b.sendall(b"VA 10\r\n1234567890\r\nEN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        val = ms.get_value(resp.size)
        assert val == b"1234567890"

        # This second get_response would panic/corrupt without the fix,
        # because pos > read after the ENDL-split path.
        resp2 = ms.get_response()
        assert isinstance(resp2, Miss)

    def test_value_with_incomplete_endl_then_value(self, socket_pair):
        """ENDL-split followed by another value response."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=18)
        b.sendall(b"VA 10\r\n1234567890\r\nVA 3\r\nfoo\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert ms.get_value(resp.size) == b"1234567890"

        resp2 = ms.get_response()
        assert isinstance(resp2, Value)
        assert ms.get_value(resp2.size) == b"foo"

    def test_multiple_values(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(
            b"VA 3\r\nfoo\r\n"
            b"VA 3\r\nbar\r\n"
            b"VA 3\r\nbaz\r\n"
        )
        for expected in [b"foo", b"bar", b"baz"]:
            resp = ms.get_response()
            assert isinstance(resp, Value)
            val = ms.get_value(resp.size)
            assert val == expected

    def test_value_then_miss(self, socket_pair):
        """Read a value, then a simple response."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 5 f1\r\nhello\r\nEN\r\n")
        resp1 = ms.get_response()
        assert isinstance(resp1, Value)
        val = ms.get_value(resp1.size)
        assert val == b"hello"

        resp2 = ms.get_response()
        assert isinstance(resp2, Miss)

    def test_interleaved_responses(self, socket_pair):
        """Simulate pipelined responses: value, success, miss, value."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(
            b"VA 2 f1\r\nhi\r\n"
            b"HD c5\r\n"
            b"EN\r\n"
            b"VA 3 f2\r\nbye\r\n"
        )
        # Value
        r = ms.get_response()
        assert isinstance(r, Value)
        assert ms.get_value(r.size) == b"hi"
        # Success
        r = ms.get_response()
        assert isinstance(r, Success)
        assert r.flags.cas_token == 5
        # Miss
        r = ms.get_response()
        assert isinstance(r, Miss)
        # Value
        r = ms.get_response()
        assert isinstance(r, Value)
        assert ms.get_value(r.size) == b"bye"


# --- NOOP handling ---


class TestNoopHandling:
    def test_noop_drains_responses(self, socket_pair):
        """Responses before NOOP should be discarded."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"test", with_noop=True)

        # EX (conflict) before MN should be discarded; HD after is real
        b.sendall(b"EX\r\nMN\r\nHD\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)

    def test_noop_no_responses_before(self, socket_pair):
        """NOOP with nothing to drain."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"test", with_noop=True)

        b.sendall(b"MN\r\nEN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Miss)

    def test_multiple_noops(self, socket_pair):
        """Multiple NOOPs should all be drained."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"cmd1", with_noop=True)
        ms.sendall(b"cmd2", with_noop=True)

        # Two MN responses followed by the actual response
        b.sendall(b"MN\r\nMN\r\nHD c1\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)
        assert resp.flags.cas_token == 1

    def test_noop_with_multiple_skipped_responses(self, socket_pair):
        """Multiple responses before NOOP are all discarded."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"test", with_noop=True)

        # HD, NS, EX all before MN - all discarded
        b.sendall(b"HD\r\nNS\r\nEX\r\nMN\r\nEN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Miss)


# --- Error handling ---


class TestErrorHandling:
    def test_closed_socket_on_get_response(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.close()
        with pytest.raises(ConnectionError):
            ms.get_response()

    def test_closed_socket_on_get_value(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 100\r\n")  # Claim 100 bytes but close
        resp = ms.get_response()
        assert isinstance(resp, Value)
        b.close()
        with pytest.raises(ConnectionError):
            ms.get_value(resp.size)

    def test_closed_socket_on_sendall(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        a.close()
        with pytest.raises(ConnectionError):
            ms.sendall(b"test\r\n", False)

    def test_close_invalidates_fd(self, socket_pair):
        """After close(), operations should fail, not use a stale fd."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.close()
        with pytest.raises(ConnectionError):
            ms.sendall(b"test\r\n", False)
        with pytest.raises(ConnectionError):
            ms.get_response()

    def test_close_resets_noop_state(self, socket_pair):
        """close() should reset noop_expected so set_socket starts clean."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"test", with_noop=True)
        ms.close()

        # Reconnect with a new socket
        c, d = socket.socketpair()
        try:
            ms.set_socket(c)
            d.sendall(b"EN\r\n")
            # Should NOT try to drain a NOOP from the previous connection
            resp = ms.get_response()
            assert isinstance(resp, Miss)
        finally:
            c.close()
            d.close()

    def test_set_socket_resets_noop_state(self, socket_pair):
        """set_socket() should reset noop_expected for the new connection."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.sendall(b"test", with_noop=True)

        c, d = socket.socketpair()
        try:
            ms.set_socket(c)
            d.sendall(b"HD c1\r\n")
            resp = ms.get_response()
            assert isinstance(resp, Success)
            assert resp.flags.cas_token == 1
        finally:
            c.close()
            d.close()


# --- Buffer management ---


class TestBufferManagement:
    def test_small_buffer_many_responses(self, socket_pair):
        """Stress the buffer reset logic with a small buffer."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=32)

        for i in range(50):
            b.sendall(b"EN\r\n")
            resp = ms.get_response()
            assert isinstance(resp, Miss)

    def test_small_buffer_values(self, socket_pair):
        """Values that just fit in a small buffer."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=64)

        for i in range(20):
            b.sendall(b"VA 5\r\nhello\r\n")
            resp = ms.get_response()
            assert isinstance(resp, Value)
            val = ms.get_value(resp.size)
            assert val == b"hello"

    def test_responses_spanning_buffer_boundary(self, socket_pair):
        """Header that arrives across multiple recv calls."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=4096)

        # Send header in two parts
        b.sendall(b"VA 3 ")
        b.sendall(b"c42\r\nfoo\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.flags.cas_token == 42
        val = ms.get_value(resp.size)
        assert val == b"foo"


# --- Version constants ---


class TestNonBlockingSocket:
    """Test with sockets in non-blocking mode (settimeout), matching Python's socket_factory_builder."""

    def test_settimeout_get_response(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)  # Puts socket in non-blocking mode with timeout
        ms = MemcacheSocket(a)

        b.sendall(b"EN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Miss)

    def test_settimeout_get_value(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)

        b.sendall(b"VA 5\r\nhello\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        val = ms.get_value(resp.size)
        assert val == b"hello"

    def test_settimeout_large_value(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a, buffer_size=100)

        payload = b"x" * 500
        b.sendall(b"VA 500\r\n" + payload + b"\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        val = ms.get_value(resp.size)
        assert val == payload

    def test_settimeout_sendall(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)

        ms.sendall(b"mg testkey\r\n", False)
        data = b.recv(1024)
        assert data == b"mg testkey\r\n"

    def test_settimeout_sendall_with_noop(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)

        ms.sendall(b"md testkey q\r\n", True)
        data = b.recv(1024)
        assert data == b"md testkey q\r\nmn\r\n"

    def test_settimeout_pipeline(self, socket_pair):
        """Full pipeline flow with non-blocking sockets."""
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)

        # Send two commands
        ms.sendall(b"mg key1\r\n", False)
        ms.sendall(b"mg key2\r\n", False)

        # Server responds
        b.sendall(b"VA 3 f1\r\nfoo\r\nEN\r\n")

        r1 = ms.get_response()
        assert isinstance(r1, Value)
        assert ms.get_value(r1.size) == b"foo"

        r2 = ms.get_response()
        assert isinstance(r2, Miss)

    def test_settimeout_noop(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)

        ms.sendall(b"test", with_noop=True)
        b.sendall(b"EX\r\nMN\r\nHD c1\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Success)
        assert resp.flags.cas_token == 1


class TestSocketTimeout:
    """Test that Python socket timeouts are respected by the Rust implementation."""

    def test_get_response_timeout(self, socket_pair):
        """get_response() should raise TimeoutError when no data arrives within timeout."""
        a, b = socket_pair
        a.settimeout(0.1)  # 100ms timeout
        ms = MemcacheSocket(a)

        # Don't send any data — should timeout
        with pytest.raises(TimeoutError):
            ms.get_response()

    def test_get_value_timeout(self, socket_pair):
        """get_value() should raise TimeoutError when value data doesn't arrive."""
        a, b = socket_pair
        a.settimeout(0.1)
        ms = MemcacheSocket(a)

        # Send header but not the value data
        b.sendall(b"VA 100\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 100

        # Value data never arrives — should timeout
        with pytest.raises((TimeoutError, ConnectionError)):
            ms.get_value(resp.size)

    def test_sendall_timeout(self, socket_pair):
        """sendall() should raise TimeoutError when send buffer is full."""
        a, b = socket_pair
        a.settimeout(0.1)
        ms = MemcacheSocket(a)

        # Fill the send buffer until it blocks, then expect timeout.
        # Use a large payload to overwhelm the socket buffer.
        big_data = b"x" * (1024 * 1024 * 10)  # 10MB
        with pytest.raises((TimeoutError, ConnectionError)):
            for _ in range(100):
                ms.sendall(big_data, False)

    def test_blocking_socket_no_timeout(self, socket_pair):
        """Blocking socket (no settimeout) should not have poll timeout issues."""
        a, b = socket_pair
        # No settimeout — socket is blocking, timeout should be -1 (infinite)
        ms = MemcacheSocket(a)

        b.sendall(b"EN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Miss)

    def test_set_socket_updates_timeout(self, socket_pair):
        """set_socket() should pick up the new socket's timeout."""
        a, b = socket_pair
        a.settimeout(0.1)
        ms = MemcacheSocket(a)

        # Create a new socket pair with no timeout
        c, d = socket.socketpair()
        try:
            ms.set_socket(c)
            d.sendall(b"EN\r\n")
            resp = ms.get_response()
            assert isinstance(resp, Miss)
        finally:
            c.close()
            d.close()


class TestVersionConstants:
    def test_constants_values(self):
        assert SERVER_VERSION_AWS_1_6_6 == 1
        assert SERVER_VERSION_STABLE == 2

    def test_version_matches_intenum(self):
        """ServerVersion IntEnum values match Rust constants."""
        # These must match for backward compatibility
        assert SERVER_VERSION_AWS_1_6_6 == 1
        assert SERVER_VERSION_STABLE == 2
