import time

import pytest

from orco import LocalExecutor


@pytest.mark.parametrize("n_processes", [None, 2])
def test_executor(env, n_processes):
    def to_dict(lst):
        return {x["id"]: x for x in lst}

    runtime = env.test_runtime()
    r = runtime.executor_summaries()
    assert len(r) == 0

    executor = LocalExecutor(heartbeat_interval=1, n_processes=n_processes)
    runtime.register_executor(executor)

    executor2 = LocalExecutor(heartbeat_interval=1, n_processes=n_processes)
    executor2._debug_do_not_start_heartbeat = True
    runtime.register_executor(executor2)

    executor3 = LocalExecutor(heartbeat_interval=1, n_processes=n_processes)
    runtime.register_executor(executor3)
    c = runtime.register_collection("abc")
    runtime.db.announce_entries(executor3.id, [c.ref("x")], [])
    assert runtime.db.get_entry_state(c.name, c.make_key("x")) == "announced"
    executor3.stop()
    assert runtime.db.get_entry_state(c.name, c.make_key("x")) is None

    r = to_dict(runtime.executor_summaries())
    assert len(r) == 3
    assert r[executor.id]["status"] == "running"
    assert r[executor2.id]["status"] == "running"
    assert r[executor3.id]["status"] == "stopped"

    time.sleep(3)

    r = to_dict(runtime.executor_summaries())
    assert len(r) == 3
    assert r[executor.id]["status"] == "running"
    assert r[executor2.id]["status"] == "lost"
    assert r[executor3.id]["status"] == "stopped"


def test_executor_error(env):
    runtime = env.test_runtime()
    executor = LocalExecutor(heartbeat_interval=1, n_processes=2)
    runtime.register_executor(executor)

    col0 = runtime.register_collection("col0", lambda c: c)
    col1 = runtime.register_collection("col1", lambda c, d: 100 // d[0].value, lambda c: [col0.ref(c)])
    col2 = runtime.register_collection("col2", lambda c, ds: sum(d.value for d in ds), lambda c: [col1.ref(x) for x in c])

    with pytest.raises(ZeroDivisionError):
        assert col2.compute([10, 0, 20])
    assert col0.get_entry_state(0) == "finished"

    assert col2.compute([10, 20]).value == 15
    assert col2.compute([1, 2, 4]).value == 175

    with pytest.raises(ZeroDivisionError):
        assert col1.compute(0)
    assert col0.get_entry_state(0) == "finished"

    assert col2.compute([10, 20]).value == 15
    assert col2.compute([1, 2, 4]).value == 175
