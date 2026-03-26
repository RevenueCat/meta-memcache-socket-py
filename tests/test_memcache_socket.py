"""Tests for the Rust MemcacheSocket class.

Mirrors the tests from meta-memcache-py/tests/memcache_socket_test.py
but tests the Rust implementation directly.
"""

import base64
import hashlib
import socket

import pytest

from meta_memcache_socket import (
    Conflict,
    MemcacheSocket,
    Miss,
    NotStored,
    RequestFlags,
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


# --- get_response: Value (now includes value bytes) ---


class TestGetResponseValue:
    def test_value_response_includes_bytes(self, socket_pair):
        """get_response() now reads value bytes automatically."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 2 c1\r\nOK\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 2
        assert resp.flags.cas_token == 1
        assert resp.value == b"OK"

    def test_value_with_all_flags(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 3 c999 h0 l60 t-1 f7 s3 W Otoken\r\nfoo\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 3
        assert resp.value == b"foo"
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
        assert resp.value == b"x"
        assert resp.flags.stale is True
        assert resp.flags.win is False

    def test_empty_value(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 0\r\n\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 0
        assert resp.value == b""

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
        assert len(resp.value) == 200
        assert resp.value == payload

    def test_value_with_incomplete_endl(self, socket_pair):
        """Buffer is just big enough for value but ENDL splits across reads."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=18)
        b.sendall(b"VA 10\r\n1234567890\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.size == 10
        assert resp.value == b"1234567890"

    def test_value_with_incomplete_endl_then_response(self, socket_pair):
        """After ENDL-split value read, buffer state must allow further responses."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=18)
        b.sendall(b"VA 10\r\n1234567890\r\nEN\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.value == b"1234567890"

        resp2 = ms.get_response()
        assert isinstance(resp2, Miss)

    def test_value_with_incomplete_endl_then_value(self, socket_pair):
        """ENDL-split followed by another value response."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=18)
        b.sendall(b"VA 10\r\n1234567890\r\nVA 3\r\nfoo\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.value == b"1234567890"

        resp2 = ms.get_response()
        assert isinstance(resp2, Value)
        assert resp2.value == b"foo"

    def test_multiple_values(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(
            b"VA 3\r\nfoo\r\n" b"VA 3\r\nbar\r\n" b"VA 3\r\nbaz\r\n"
        )
        for expected in [b"foo", b"bar", b"baz"]:
            resp = ms.get_response()
            assert isinstance(resp, Value)
            assert resp.value == expected

    def test_value_then_miss(self, socket_pair):
        """Read a value, then a simple response."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 5 f1\r\nhello\r\nEN\r\n")
        resp1 = ms.get_response()
        assert isinstance(resp1, Value)
        assert resp1.value == b"hello"

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
        assert r.value == b"hi"
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
        assert r.value == b"bye"


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
        assert resp.value == b"OK"

        resp2 = ms.get_response()
        assert isinstance(resp2, Success)
        assert resp2.flags.cas_token == 2


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
            assert resp.value == b"hello"

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
        assert resp.value == b"foo"


# --- send_meta_* (Tier 1: pipelining) ---


class TestSendMeta:
    def test_send_meta_get(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_cas_token=True, cache_ttl=300)
        ms.send_meta_get(b"mykey", flags)
        data = b.recv(1024)
        assert data == b"mg mykey c T300\r\n"

    def test_send_meta_get_no_flags(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.send_meta_get(b"mykey")
        data = b.recv(1024)
        assert data == b"mg mykey\r\n"

    def test_send_meta_set(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(cache_ttl=300, client_flag=0)
        ms.send_meta_set(b"mykey", b"hello", flags)
        data = b.recv(1024)
        assert data == b"ms mykey 5 T300 F0\r\nhello\r\n"

    def test_send_meta_set_with_noop(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.send_meta_set(b"mykey", b"hello")
        data_without = b.recv(1024)
        assert data_without == b"ms mykey 5\r\nhello\r\n"

    def test_send_meta_set_with_noop_flag(self, socket_pair):
        """no_reply flag auto-injects NOOP."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(no_reply=True)
        ms.send_meta_set(b"mykey", b"hi", flags)
        data = b.recv(1024)
        assert data == b"ms mykey 2 q\r\nhi\r\nmn\r\n"

    def test_send_meta_set_empty_value(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        ms.send_meta_set(b"mykey", b"")
        data = b.recv(1024)
        assert data == b"ms mykey 0\r\n\r\n"

    def test_send_meta_delete(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(cache_ttl=300)
        ms.send_meta_delete(b"mykey", flags)
        data = b.recv(1024)
        assert data == b"md mykey T300\r\n"

    def test_send_meta_arithmetic(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(ma_delta_value=5)
        ms.send_meta_arithmetic(b"mykey", flags)
        data = b.recv(1024)
        assert data == b"ma mykey D5\r\n"

    def test_send_meta_get_empty_key(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        with pytest.raises(ValueError):
            ms.send_meta_get(b"")

    def test_pipeline_send_then_recv(self, socket_pair):
        """Full pipeline: send multiple, then recv multiple."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_cas_token=True)

        # Send 3 gets
        ms.send_meta_get(b"key1", flags)
        ms.send_meta_get(b"key2", flags)
        ms.send_meta_get(b"key3", flags)

        # Server responds
        b.sendall(
            b"VA 3 c1\r\nfoo\r\n"
            b"EN\r\n"
            b"VA 3 c3\r\nbar\r\n"
        )

        r1 = ms.get_response()
        assert isinstance(r1, Value)
        assert r1.value == b"foo"
        assert r1.flags.cas_token == 1

        r2 = ms.get_response()
        assert isinstance(r2, Miss)

        r3 = ms.get_response()
        assert isinstance(r3, Value)
        assert r3.value == b"bar"
        assert r3.flags.cas_token == 3


# --- meta_* (Tier 3: blocking) ---


class TestMetaBlocking:
    def test_meta_get_miss(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        # Server responds with miss after receiving get
        b.sendall(b"EN\r\n")
        resp = ms.meta_get(b"mykey")
        assert isinstance(resp, Miss)

    def test_meta_get_value(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_cas_token=True, return_value=True)
        b.sendall(b"VA 5 c42\r\nhello\r\n")
        resp = ms.meta_get(b"mykey", flags)
        assert isinstance(resp, Value)
        assert resp.value == b"hello"
        assert resp.flags.cas_token == 42

    def test_meta_get_verifies_wire(self, socket_pair):
        """meta_get sends the correct wire format."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_cas_token=True, cache_ttl=300)
        # Need to respond so the blocking call doesn't hang
        b.sendall(b"EN\r\n")
        ms.meta_get(b"testkey", flags)
        # Check what was sent
        data = b.recv(1024)
        assert data == b"mg testkey c T300\r\n"

    def test_meta_set_success(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(cache_ttl=300, client_flag=0)
        b.sendall(b"HD c1\r\n")
        resp = ms.meta_set(b"mykey", b"hello", flags)
        assert isinstance(resp, Success)
        assert resp.flags.cas_token == 1
        # Check wire format
        data = b.recv(1024)
        assert data == b"ms mykey 5 T300 F0\r\nhello\r\n"

    def test_meta_set_no_reply(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(no_reply=True, cache_ttl=300)
        resp = ms.meta_set(b"mykey", b"hello", flags)
        assert isinstance(resp, Success)
        # Check wire format includes noop
        data = b.recv(1024)
        assert data == b"ms mykey 5 q T300\r\nhello\r\nmn\r\n"

    def test_meta_set_not_stored(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"NS\r\n")
        resp = ms.meta_set(b"mykey", b"hello")
        assert isinstance(resp, NotStored)

    def test_meta_delete_success(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"HD\r\n")
        resp = ms.meta_delete(b"mykey")
        assert isinstance(resp, Success)
        data = b.recv(1024)
        assert data == b"md mykey\r\n"

    def test_meta_delete_no_reply(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(no_reply=True)
        resp = ms.meta_delete(b"mykey", flags)
        assert isinstance(resp, Success)
        data = b.recv(1024)
        assert data == b"md mykey q\r\nmn\r\n"

    def test_meta_delete_miss(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"NF\r\n")
        resp = ms.meta_delete(b"mykey")
        assert isinstance(resp, Miss)

    def test_meta_arithmetic_success(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(ma_delta_value=5, return_value=True)
        b.sendall(b"VA 2\r\n10\r\n")
        resp = ms.meta_arithmetic(b"counter", flags)
        assert isinstance(resp, Value)
        assert resp.value == b"10"
        data = b.recv(1024)
        assert data == b"ma counter v D5\r\n"

    def test_meta_arithmetic_no_reply(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(no_reply=True, ma_delta_value=1)
        resp = ms.meta_arithmetic(b"counter", flags)
        assert isinstance(resp, Success)
        data = b.recv(1024)
        assert data == b"ma counter q D1\r\nmn\r\n"

    def test_meta_get_invalid_key(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        with pytest.raises(ValueError):
            ms.meta_get(b"")

    def test_meta_set_legacy_version(self, socket_pair):
        """AWS 1.6.6 uses legacy size format with S prefix."""
        a, b = socket_pair
        ms = MemcacheSocket(a, version=SERVER_VERSION_AWS_1_6_6)
        b.sendall(b"HD\r\n")
        ms.meta_set(b"mykey", b"hello")
        data = b.recv(1024)
        assert data == b"ms mykey S5\r\nhello\r\n"

    def test_meta_no_reply_then_regular(self, socket_pair):
        """no_reply command followed by regular command should work
        (noop draining is handled correctly)."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        # no_reply delete
        flags_noreply = RequestFlags(no_reply=True)
        resp1 = ms.meta_delete(b"key1", flags_noreply)
        assert isinstance(resp1, Success)

        # Regular get — server sends noop response (from delete), then miss
        b.sendall(b"MN\r\nEN\r\n")
        resp2 = ms.meta_get(b"key2")
        # The MN should be drained, and we get the EN (Miss)
        assert isinstance(resp2, Miss)


# --- Non-blocking sockets ---


class TestNonBlockingSocket:
    """Test with sockets in non-blocking mode (settimeout)."""

    def test_settimeout_get_response(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
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
        assert resp.value == b"hello"

    def test_settimeout_large_value(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a, buffer_size=100)

        payload = b"x" * 500
        b.sendall(b"VA 500\r\n" + payload + b"\r\n")
        resp = ms.get_response()
        assert isinstance(resp, Value)
        assert resp.value == payload

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
        ms.send_meta_get(b"key1")
        ms.send_meta_get(b"key2")

        # Server responds
        b.sendall(b"VA 3 f1\r\nfoo\r\nEN\r\n")

        r1 = ms.get_response()
        assert isinstance(r1, Value)
        assert r1.value == b"foo"

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

    def test_settimeout_meta_blocking(self, socket_pair):
        """Blocking meta_* with non-blocking sockets."""
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)

        b.sendall(b"VA 5 c1\r\nhello\r\n")
        resp = ms.meta_get(b"mykey", RequestFlags(return_cas_token=True, return_value=True))
        assert isinstance(resp, Value)
        assert resp.value == b"hello"
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

    def test_sendall_timeout(self, socket_pair):
        """sendall() should raise TimeoutError when send buffer is full."""
        a, b = socket_pair
        a.settimeout(0.1)
        ms = MemcacheSocket(a)

        # Fill the send buffer until it blocks, then expect timeout.
        big_data = b"x" * (1024 * 1024 * 10)  # 10MB
        with pytest.raises((TimeoutError, ConnectionError)):
            for _ in range(100):
                ms.sendall(big_data, False)

    def test_blocking_socket_no_timeout(self, socket_pair):
        """Blocking socket (no settimeout) should not have poll timeout issues."""
        a, b = socket_pair
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


# --- meta_multiget (batch) ---


class TestMetaMultiget:
    def test_empty_keys(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        results = ms.meta_multiget([])
        assert results == []

    def test_single_key_hit(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 5 c1\r\nhello\r\n")
        results = ms.meta_multiget([b"key1"], RequestFlags(return_cas_token=True, return_value=True))
        assert len(results) == 1
        assert isinstance(results[0], Value)
        assert results[0].value == b"hello"
        assert results[0].flags.cas_token == 1

    def test_single_key_miss(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EN\r\n")
        results = ms.meta_multiget([b"key1"])
        assert len(results) == 1
        assert isinstance(results[0], Miss)

    def test_multiple_keys_all_hits(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_cas_token=True, return_value=True)
        b.sendall(
            b"VA 3 c1\r\nfoo\r\n"
            b"VA 3 c2\r\nbar\r\n"
            b"VA 3 c3\r\nbaz\r\n"
        )
        results = ms.meta_multiget([b"k1", b"k2", b"k3"], flags)
        assert len(results) == 3
        assert all(isinstance(r, Value) for r in results)
        assert results[0].value == b"foo"
        assert results[0].flags.cas_token == 1
        assert results[1].value == b"bar"
        assert results[1].flags.cas_token == 2
        assert results[2].value == b"baz"
        assert results[2].flags.cas_token == 3

    def test_multiple_keys_all_misses(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EN\r\n" * 5)
        results = ms.meta_multiget([b"k1", b"k2", b"k3", b"k4", b"k5"])
        assert len(results) == 5
        assert all(isinstance(r, Miss) for r in results)

    def test_mixed_hits_and_misses(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_value=True)
        b.sendall(
            b"VA 3 f1\r\nfoo\r\n"
            b"EN\r\n"
            b"VA 3 f2\r\nbar\r\n"
            b"EN\r\n"
            b"VA 3 f3\r\nbaz\r\n"
        )
        results = ms.meta_multiget([b"k1", b"k2", b"k3", b"k4", b"k5"], flags)
        assert len(results) == 5
        assert isinstance(results[0], Value)
        assert results[0].value == b"foo"
        assert isinstance(results[1], Miss)
        assert isinstance(results[2], Value)
        assert results[2].value == b"bar"
        assert isinstance(results[3], Miss)
        assert isinstance(results[4], Value)
        assert results[4].value == b"baz"

    def test_verifies_wire_format(self, socket_pair):
        """meta_multiget sends correct wire commands."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_cas_token=True, cache_ttl=300)
        b.sendall(b"EN\r\nEN\r\nEN\r\n")
        ms.meta_multiget([b"key1", b"key2", b"key3"], flags)
        data = b.recv(4096)
        assert data == (
            b"mg key1 c T300\r\n"
            b"mg key2 c T300\r\n"
            b"mg key3 c T300\r\n"
        )

    def test_string_keys(self, socket_pair):
        """String keys should work (extracted as UTF-8 bytes)."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(return_value=True)
        b.sendall(b"VA 2\r\nhi\r\nEN\r\n")
        results = ms.meta_multiget(["mykey", "other"], flags)
        assert len(results) == 2
        assert isinstance(results[0], Value)
        assert results[0].value == b"hi"
        assert isinstance(results[1], Miss)
        # Verify wire format
        data = b.recv(4096)
        assert data == b"mg mykey v\r\nmg other v\r\n"

    def test_mixed_key_types(self, socket_pair):
        """Mix of str and bytes keys."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EN\r\nEN\r\n")
        results = ms.meta_multiget(["strkey", b"byteskey"])
        assert len(results) == 2
        data = b.recv(4096)
        assert data == b"mg strkey\r\nmg byteskey\r\n"

    def test_empty_key_raises(self, socket_pair):
        a, b = socket_pair
        ms = MemcacheSocket(a)
        with pytest.raises(ValueError):
            ms.meta_multiget([b"good", b"", b"also_good"])

    def test_large_value_in_batch(self, socket_pair):
        """Values larger than buffer should work in batch mode."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=100)
        payload = b"x" * 200
        b.sendall(
            b"VA 200\r\n" + payload + b"\r\n"
            b"EN\r\n"
            b"VA 3\r\nfoo\r\n"
        )
        results = ms.meta_multiget([b"k1", b"k2", b"k3"], RequestFlags(return_value=True))
        assert len(results) == 3
        assert isinstance(results[0], Value)
        assert results[0].value == payload
        assert isinstance(results[1], Miss)
        assert isinstance(results[2], Value)
        assert results[2].value == b"foo"

    def test_small_buffer_many_keys(self, socket_pair):
        """Stress buffer reset logic with many keys and small buffer."""
        a, b = socket_pair
        ms = MemcacheSocket(a, buffer_size=32)
        num_keys = 50
        keys = [f"k{i}".encode() for i in range(num_keys)]
        # Alternate hits and misses
        response = b""
        for i in range(num_keys):
            if i % 2 == 0:
                response += b"VA 1\r\n" + str(i % 10).encode() + b"\r\n"
            else:
                response += b"EN\r\n"
        b.sendall(response)
        results = ms.meta_multiget(keys, RequestFlags(return_value=True))
        assert len(results) == num_keys
        for i, r in enumerate(results):
            if i % 2 == 0:
                assert isinstance(r, Value), f"Expected Value at index {i}"
                assert r.value == str(i % 10).encode()
            else:
                assert isinstance(r, Miss), f"Expected Miss at index {i}"

    def test_multiget_then_regular_get(self, socket_pair):
        """Socket state should be clean after meta_multiget."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VA 3\r\nfoo\r\nEN\r\n")
        results = ms.meta_multiget([b"k1", b"k2"])
        assert len(results) == 2

        # Regular get should still work
        b.sendall(b"VA 3 c99\r\nbar\r\n")
        resp = ms.meta_get(b"k3", RequestFlags(return_cas_token=True))
        assert isinstance(resp, Value)
        assert resp.value == b"bar"
        assert resp.flags.cas_token == 99

    def test_noop_draining_before_multiget(self, socket_pair):
        """Pending NOOPs should be drained before multiget responses."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        # Send a no_reply delete (injects noop)
        flags_noreply = RequestFlags(no_reply=True)
        ms.meta_delete(b"old_key", flags_noreply)

        # Now do a multiget — server sends noop from delete, then multiget responses
        b.sendall(b"MN\r\nVA 2\r\nhi\r\nEN\r\n")
        results = ms.meta_multiget([b"k1", b"k2"])
        assert len(results) == 2
        assert isinstance(results[0], Value)
        assert results[0].value == b"hi"
        assert isinstance(results[1], Miss)

    def test_with_flags(self, socket_pair):
        """All response flags should be correctly parsed in batch mode."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        flags = RequestFlags(
            return_cas_token=True,
            return_value=True,
            return_ttl=True,
            return_client_flag=True,
        )
        b.sendall(
            b"VA 3 c10 t300 f42\r\nabc\r\n"
            b"VA 2 c20 t600 f99\r\nxy\r\n"
        )
        results = ms.meta_multiget([b"k1", b"k2"], flags)
        assert len(results) == 2
        assert results[0].flags.cas_token == 10
        assert results[0].flags.ttl == 300
        assert results[0].flags.client_flag == 42
        assert results[0].value == b"abc"
        assert results[1].flags.cas_token == 20
        assert results[1].flags.ttl == 600
        assert results[1].flags.client_flag == 99
        assert results[1].value == b"xy"

    def test_timeout_socket(self, socket_pair):
        """meta_multiget works with sockets that have a timeout set."""
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)
        b.sendall(
            b"VA 3\r\nfoo\r\n"
            b"EN\r\n"
        )
        results = ms.meta_multiget([b"k1", b"k2"])
        assert len(results) == 2
        assert isinstance(results[0], Value)
        assert results[0].value == b"foo"
        assert isinstance(results[1], Miss)


# --- String key encoding ---


class TestStringKeys:
    def test_small_ascii_str_key(self, socket_pair):
        """Plain ASCII string key passes through as UTF-8 bytes unchanged."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"EN\r\n")
        ms.meta_get("hello")
        data = b.recv(1024)
        assert data == b"mg hello\r\n"

    def test_large_str_key_hashed(self, socket_pair):
        """String key >= 187 chars is blake2b-hashed and base64-encoded with 'b' flag."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        key = "k" * 200
        b.sendall(b"EN\r\n")
        ms.meta_get(key)
        data = b.recv(1024)
        digest = hashlib.blake2b(key.encode(), digest_size=18).digest()
        expected_b64 = base64.b64encode(digest)
        assert data == b"mg " + expected_b64 + b" b\r\n"

    def test_unicode_str_key(self, socket_pair):
        """Unicode key is UTF-8 encoded then base64-encoded with 'b' flag."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        key = "caf\u00e9"  # "café" — non-ASCII byte \xc3\xa9
        b.sendall(b"EN\r\n")
        ms.meta_get(key)
        data = b.recv(1024)
        expected_b64 = base64.b64encode(key.encode("utf-8"))
        assert data == b"mg " + expected_b64 + b" b\r\n"

    def test_str_key_meta_set(self, socket_pair):
        """meta_set accepts a str key (ASCII)."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"HD\r\n")
        ms.meta_set("mykey", b"value")
        data = b.recv(1024)
        assert data == b"ms mykey 5\r\nvalue\r\n"

    def test_str_key_meta_delete(self, socket_pair):
        """meta_delete accepts a str key (ASCII)."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"HD\r\n")
        ms.meta_delete("mykey")
        data = b.recv(1024)
        assert data == b"md mykey\r\n"

    def test_unicode_str_key_meta_set(self, socket_pair):
        """meta_set with a unicode key base64-encodes it and adds 'b' flag."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        key = "\u6d4b\u8bd5"  # "测试" (Chinese, 6 UTF-8 bytes)
        b.sendall(b"HD\r\n")
        ms.meta_set(key, b"val")
        data = b.recv(1024)
        expected_b64 = base64.b64encode(key.encode("utf-8"))
        assert data == b"ms " + expected_b64 + b" 3 b\r\nval\r\n"


# --- raw_cmd ---


class TestRawCmd:
    def test_single_line_version(self, socket_pair):
        """Typical single-line command like 'version'."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"VERSION 1.6.22\r\n")
        result = ms.raw_cmd(b"version")
        assert result == b"VERSION 1.6.22"
        # Verify \r\n was appended
        data = b.recv(1024)
        assert data == b"version\r\n"

    def test_single_line_already_has_endl(self, socket_pair):
        """Command already ending with \\r\\n should not get doubled."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"OK\r\n")
        result = ms.raw_cmd(b"flush_all\r\n")
        assert result == b"OK"
        data = b.recv(1024)
        assert data == b"flush_all\r\n"

    def test_multi_line_stats(self, socket_pair):
        """Multi-line response like 'stats'."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(
            b"STAT pid 12345\r\n"
            b"STAT uptime 1000\r\n"
            b"STAT version 1.6.22\r\n"
            b"END\r\n"
        )
        result = ms.raw_cmd(b"stats", multi_line=True)
        assert result == (
            b"STAT pid 12345\r\n"
            b"STAT uptime 1000\r\n"
            b"STAT version 1.6.22\r\n"
        )

    def test_multi_line_empty(self, socket_pair):
        """Multi-line response with no content before END."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"END\r\n")
        result = ms.raw_cmd(b"stats slabs", multi_line=True)
        assert result == b""

    def test_single_line_empty_response(self, socket_pair):
        """Server returns just \\r\\n."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        b.sendall(b"\r\n")
        result = ms.raw_cmd(b"test")
        assert result == b""

    def test_nonblocking_socket(self, socket_pair):
        a, b = socket_pair
        a.settimeout(5.0)
        ms = MemcacheSocket(a)
        b.sendall(b"VERSION 1.6.22\r\n")
        result = ms.raw_cmd(b"version")
        assert result == b"VERSION 1.6.22"

    def test_does_not_affect_main_buffer(self, socket_pair):
        """raw_cmd should not disturb main I/O state for subsequent meta commands."""
        a, b = socket_pair
        ms = MemcacheSocket(a)
        # raw command
        b.sendall(b"VERSION 1.6.22\r\n")
        ms.raw_cmd(b"version")
        # meta get should still work
        b.sendall(b"VA 3 c1\r\nfoo\r\n")
        resp = ms.meta_get(b"mykey", RequestFlags(return_cas_token=True))
        assert isinstance(resp, Value)
        assert resp.value == b"foo"
        assert resp.flags.cas_token == 1


class TestVersionConstants:
    def test_constants_values(self):
        assert SERVER_VERSION_AWS_1_6_6 == 1
        assert SERVER_VERSION_STABLE == 2

    def test_version_matches_intenum(self):
        """ServerVersion IntEnum values match Rust constants."""
        assert SERVER_VERSION_AWS_1_6_6 == 1
        assert SERVER_VERSION_STABLE == 2
