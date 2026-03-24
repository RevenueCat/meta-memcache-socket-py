"""Tests for Rust response types: Value, Success, Miss, NotStored, Conflict."""

from meta_memcache_socket import (
    Conflict,
    Miss,
    NotStored,
    ResponseFlags,
    Success,
    Value,
)


class TestMiss:
    def test_create(self):
        m = Miss()
        assert repr(m) == "Miss()"

    def test_bool_is_false(self):
        assert not Miss()
        assert bool(Miss()) is False

    def test_equality(self):
        assert Miss() == Miss()

    def test_inequality_with_other_types(self):
        assert Miss() != NotStored()
        assert Miss() != Conflict()


class TestNotStored:
    def test_create(self):
        ns = NotStored()
        assert repr(ns) == "NotStored()"

    def test_bool_is_false(self):
        assert not NotStored()

    def test_equality(self):
        assert NotStored() == NotStored()


class TestConflict:
    def test_create(self):
        c = Conflict()
        assert repr(c) == "Conflict()"

    def test_bool_is_false(self):
        assert not Conflict()

    def test_equality(self):
        assert Conflict() == Conflict()


class TestSuccess:
    def test_create(self):
        flags = ResponseFlags()
        s = Success(flags)
        assert s.flags == flags

    def test_create_with_flags(self):
        flags = ResponseFlags(cas_token=42, stale=True)
        s = Success(flags)
        assert s.flags.cas_token == 42
        assert s.flags.stale is True

    def test_repr(self):
        s = Success(ResponseFlags())
        assert "Success" in repr(s)

    def test_flags_immutable(self):
        """Success is frozen, flags should not be settable."""
        s = Success(ResponseFlags())
        try:
            s.flags = ResponseFlags(cas_token=1)
            assert False, "Should have raised AttributeError"
        except AttributeError:
            pass


class TestValue:
    def test_create(self):
        flags = ResponseFlags(client_flag=42)
        v = Value(size=100, flags=flags, value=None)
        assert v.size == 100
        assert v.flags.client_flag == 42
        assert v.value is None

    def test_value_setter(self):
        """Value.value must be settable (used by executor to attach deserialized data)."""
        v = Value(size=5, flags=ResponseFlags(), value=None)
        assert v.value is None
        v.value = b"hello"
        assert v.value == b"hello"
        v.value = "deserialized string"
        assert v.value == "deserialized string"
        v.value = {"key": "val"}
        assert v.value == {"key": "val"}
        v.value = None
        assert v.value is None

    def test_repr(self):
        v = Value(size=10, flags=ResponseFlags(), value=None)
        r = repr(v)
        assert "Value" in r
        assert "10" in r

    def test_isinstance_checks(self):
        """isinstance checks must work (used extensively by executor/commands)."""
        v = Value(size=1, flags=ResponseFlags(), value=None)
        s = Success(ResponseFlags())
        m = Miss()
        ns = NotStored()
        c = Conflict()

        assert isinstance(v, Value)
        assert isinstance(s, Success)
        assert isinstance(m, Miss)
        assert isinstance(ns, NotStored)
        assert isinstance(c, Conflict)

        # Value is NOT a subclass of Success in Rust version
        assert not isinstance(v, Success)
        assert not isinstance(s, Value)
        assert not isinstance(m, Value)
