import time
import threading
import pytest


from orco import LocalExecutor


@pytest.mark.parametrize("n_processes", [1, 2])
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


def test_executor_fallible(env):
    runtime = env.test_runtime()
    executor = LocalExecutor(heartbeat_interval=1, n_processes=2, skip_errors=True)
    runtime.register_executor(executor)

    counter = env.file_storage("counter", 0)

    def build_fn(c):
        if c == 0:
            if counter.read() == 0:
                counter.write(counter.read() + 1)
                return 1 / c
            else:
                return 1
        else:
            return c

    col0 = runtime.register_collection("col0", build_fn)

    col0.compute_many([10, 0, 20])
    assert col0.get_entry_state(0) is None
    assert [e.value for e in col0.compute_many([10, 0, 20])] == [10, 1, 20]


def test_executor_conflict(env, tmpdir):

    def compute_0(c):
        path = tmpdir.join("test-{}".format(c))
        assert not path.check()
        path.write("Done")
        time.sleep(1)
        return c

    def compute_1(c, d):
        return sum([x.value for x in d])

    def init():
        runtime = env.test_runtime()
        executor = LocalExecutor(heartbeat_interval=1, n_processes=1)
        runtime.register_executor(executor)
        col0 = runtime.register_collection("col0", compute_0)
        col1 = runtime.register_collection("col1", compute_1, lambda c: [col0.ref(x) for x in c])
        return runtime, col0, col1

    runtime1, col0_0, col1_0 = init()
    runtime2, col0_1, col1_1 = init()

    results = [None, None]

    def comp1(runtime, col0, col1):
        results[0] = col1.compute([2,3,7,10])

    def comp2(runtime, col0, col1):
        results[1] = col1.compute([2,3,7,11])

    t1 = threading.Thread(target=comp1, args=(runtime1, col0_0, col1_0))
    t1.start()
    time.sleep(0.5)
    t2 = threading.Thread(target=comp2, args=(runtime1, col1_0, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 22
    assert results[1].value == 23

    assert tmpdir.join("test-10").mtime() > tmpdir.join("test-11").mtime()

    results = [None, None]

    def comp3(runtime, col0, col1):
        results[0] = col1.compute([2,7,10, 30])

    def comp4(runtime, col0, col1):
        results[1] = col0.compute(30)

    t1 = threading.Thread(target=comp3, args=(runtime1, col0_0, col1_0))
    t1.start()
    t2 = threading.Thread(target=comp4, args=(runtime1, col0_1, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 49
    assert results[1].value == 30


    t1 = threading.Thread(target=comp1, args=(runtime1, col0_0, col1_0))
    t1.start()
    t2 = threading.Thread(target=comp2, args=(runtime1, col1_0, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 22
    assert results[1].value == 23