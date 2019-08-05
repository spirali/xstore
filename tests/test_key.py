from orco.collection import default_make_key
import pytest


def test_default_make_key_basics():
    assert default_make_key(10) == "10"
    assert default_make_key("Hello!") == "'Hello!'"
    assert default_make_key(3.14) == "3.14"

    assert default_make_key({"x": 10, "y": 20}) == default_make_key({"y": 20, "x": 10})
    assert default_make_key({"x": 10, "y": 20}) != default_make_key({"y": 10, "x": 20})

    assert default_make_key((10, 20)) == default_make_key([10, 20])
    assert default_make_key([10, 20]) != default_make_key([20, 10])


def test_default_make_key_ignored_keys():
    assert default_make_key({"x": 10, "_not_here": 20}) == default_make_key({"x": 10})
    assert default_make_key({"x": 10, "_not_here": 20}) == default_make_key({"x": 10, "_no_here": 30})
    assert default_make_key({"x": 10, "_not_here": 20}) == default_make_key({"x": 10, "_no_here2": 40})


def test_default_make_key_invalid():

    class X():
        pass

    with pytest.raises(Exception):
        default_make_key(["10", X()])

    with pytest.raises(Exception):
        default_make_key([X()])

    with pytest.raises(Exception):
        default_make_key({10: 10})
