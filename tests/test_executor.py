import time
import threading
import pytest

from orco import LocalExecutor, CollectionRef, TaskFailException
from orco.ref import make_key


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
    assert runtime.db.get_entry_state(c.name, make_key("x")) == "announced"
    executor3.stop()
    assert runtime.db.get_entry_state(c.name, make_key("x")) is None

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

    col0 = runtime.register_collection("col0", lambda c, d: c)
    col1 = runtime.register_collection("col1", lambda c, d: 100 // d[0].value,
                                       lambda c: [col0.ref(c)])
    col2 = runtime.register_collection("col2", lambda c, ds: sum(d.value for d in ds),
                                       lambda c: [col1.ref(x) for x in c])

    assert not runtime.get_reports()

    with pytest.raises(TaskFailException, match=".*ZeroDivisionError.*"):
        assert runtime.compute(col2.ref([10, 0, 20]))

    reports = runtime.get_reports()
    assert len(reports) == 2
    assert reports[0].report_type == "error"
    assert reports[0].collection_name == "col1"
    assert reports[0].config == 0
    assert "ZeroDivisionError" in reports[0].message

    assert runtime.get_entry_state(col0.ref(0)) == "finished"

    assert runtime.compute(col2.ref([10, 20])).value == 15
    assert runtime.compute(col2.ref([1, 2, 4])).value == 175

    with pytest.raises(TaskFailException, match=".*ZeroDivisionError.*"):
        assert runtime.compute(col1.ref(0))
    assert runtime.get_entry_state(col0.ref(0)) == "finished"

    assert runtime.compute(col2.ref([10, 20])).value == 15
    assert runtime.compute(col2.ref([1, 2, 4])).value == 175

    r1 = [col2.ref([100 + x, 101 + x, 102 + x]) for x in range(20)]
    r2 = [col2.ref([200 + x, 201 + x, 202 + x]) for x in range(20)]
    result = runtime.compute(r1 + [col2.ref([303, 0, 304])] + r2, continue_on_error=True)
    for i in range(20):
        assert result[i] is not None
    for i in range(21, 41):
        assert result[i] is not None
    assert result[20] is None
    reports = runtime.get_reports()
    reports[0].report_type = "error"
    reports[0].config = [303, 0, 304]
    print(result)


def test_executor_conflict(env, tmpdir):

    def compute_0(c, d):
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
        results[0] = runtime1.compute(col1.ref([0, 2, 3, 7, 10]))

    def comp2(runtime, col0, col1):
        results[1] = runtime2.compute(col1.ref([2, 3, 7, 11]))

    t1 = threading.Thread(target=comp1, args=(runtime1, col0_0, col1_0))
    t1.start()
    time.sleep(0.5)
    assert runtime2.get_entry_state(CollectionRef("col0").ref(0)) == "announced"

    t2 = threading.Thread(target=comp2, args=(runtime2, col0_1, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 22
    assert results[1].value == 23

    assert tmpdir.join("test-10").mtime() > tmpdir.join("test-11").mtime()

    results = [None, None]

    def comp3(runtime, col0, col1):
        results[0] = runtime1.compute(col1.ref([2, 7, 10, 30]))

    def comp4(runtime, col0, col1):
        results[1] = runtime2.compute(col0.ref(30))

    t1 = threading.Thread(target=comp3, args=(runtime1, col0_0, col1_0))
    t1.start()
    t2 = threading.Thread(target=comp4, args=(runtime2, col0_1, col1_1))
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
