from orco import computation


def test_runtime_get_entries(runtime):

    @computation()
    def my_fn(x):
        return x * 10
