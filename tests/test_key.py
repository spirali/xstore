import pytest

from orco.internals.key import make_key


def test_make_key_basics():
    assert make_key(10) == "10"
    assert make_key("Hello!") == "'Hello!'"
    assert make_key("\'\"Hello!") == "'\\'\"Hello!'"
    assert make_key(3.14) == "3.14"
    assert make_key([True, False, 2]) == "[True,False,2,]"

    assert make_key({"x": 10, "y": 20}) == make_key({"y": 20, "x": 10})
    assert make_key({"x": 10, "y": 20}) != make_key({"y": 10, "x": 20})

    assert make_key((10, 20)) == make_key([10, 20])
    assert make_key([10, 20]) != make_key([20, 10])


def test_make_key_ignored_keys():
    assert make_key({"x": 10, "_not_here": 20}) == make_key({"x": 10})
    assert make_key({"x": 10, "_not_here": 20}) == make_key({"x": 10, "_no_here": 30})
    assert make_key({"x": 10, "_not_here": 20}) == make_key({"x": 10, "_no_here2": 40})


def test_make_key_invalid():
    class X():
        pass

    with pytest.raises(Exception):
        make_key(["10", X()])

    with pytest.raises(Exception):
        make_key([X()])

    with pytest.raises(Exception):
        make_key({10: 10})
