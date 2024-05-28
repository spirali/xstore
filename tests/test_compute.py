from orco import computation


def test_simple_compute(runtime):
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
