from orco import computation
import pytest


def test_compute_simple(runtime):
    counter = {}

    @computation()
    def my_fn(x, y):
        counter.setdefault((x, y), 0)
        counter[(x, y)] += 1
        return x * 10 + y

    with runtime:
        assert my_fn(10, 1) == 101
        assert my_fn(10, 1) == 101
        assert my_fn(1, 10) == 20
        assert my_fn(1, 10) == 20
        assert my_fn(10, 1) == 101

    assert counter[(10, 1)] == 1
    assert counter[(1, 10)] == 1


class MyException(Exception):
    pass


def test_compute_fail(runtime):
    flag = True

    @computation()
    def my_fn(x):
        if flag:
            raise MyException()
        return x * 2

    with runtime:
        with pytest.raises(MyException):
            my_fn(10)
        with pytest.raises(MyException):
            my_fn(10)
        flag = False
        assert my_fn(10) == 20
        flag = True
        assert my_fn(10) == 20


def test_compute_deps(runtime):
    @computation()
    def my_fn2(x, y):
        a = my_fn1(x)
        b = my_fn1(y)
        return a, b

    @computation()
    def my_fn1(x):
        return x * my_fn0()

    @computation()
    def my_fn0():
        return 10

    with runtime:
        assert runtime.get(my_fn0.ref()) is None
        assert my_fn2(1, 3) == (10, 30)
        assert runtime.get([my_fn1.ref(1), my_fn1.ref(2), my_fn1.ref(3)]) == [
            10,
            None,
            30,
        ]
        assert runtime.get(my_fn0.ref()) == 10


def test_compute_none_result(runtime):
    counter = [0]

    @computation()
    def my_fn():
        counter[0] += 1
        return None

    with runtime:
        assert my_fn() is None
        assert my_fn() is None
        assert my_fn() is None

    assert counter[0] == 1
