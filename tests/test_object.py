from orco import Obj

import pytest


def test_object_creation():
    o = Obj(x=10, abc_xyz="test test")

    assert o.x == 10
    assert o.abc_xyz == "test test"

    o = Obj({"x": 10, "abc_xyz": "test test"})

    assert o.x == 10
    assert o.abc_xyz == "test test"


def test_invalid_object():
    with pytest.raises(Exception):
        Obj({"a b": 10})

    with pytest.raises(Exception):
        Obj({10: 10})

    with pytest.raises(Exception):
        Obj({"10": 10})
