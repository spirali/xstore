
import pytest
import pickle

from orco import LocalExecutor


def adder(config):
    return config["a"] + config["b"]


def test_reopen_collection(env):
    runtime = env.test_runtime()
    runtime.register_collection("col1", adder)

    with pytest.raises(Exception):
        runtime.register_collection("col1", adder)


def test_fixed_collection(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    fix1 = runtime.register_collection("fix1")
    col2 = runtime.register_collection("col2", lambda c, d: d[0].value * 10, lambda c: [fix1.ref(c)])

    fix1.insert("a", 11)

    assert col2.compute("a").value == 110
    assert fix1.compute("a").value == 11

    with pytest.raises(Exception, match=".* fixed collection.*"):
        assert col2.compute("b")
    with pytest.raises(Exception, match=".* fixed collection.*"):
        assert fix1.compute("b")


def test_collection_compute(env, tmpdir):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))

    counter_file = tmpdir.join("counter")
    counter_file.write_binary(pickle.dumps(0))

    def adder(config):
        counter = pickle.loads(counter_file.read_binary())
        counter += 1
        counter_file.write_binary(pickle.dumps(counter))
        return config["a"] + config["b"]

    collection = runtime.register_collection("col1", adder)

    entry = collection.compute({"a": 10, "b": 30})
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    counter = pickle.loads(counter_file.read_binary())
    assert counter == 1

    result = collection.compute_many([{"a": 10, "b": 30}])
    assert len(result) == 1
    entry = result[0]
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert counter == 1


def test_collection_deps(env, tmpdir):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))
    counter = [0, 0]

    counter_file = tmpdir.join("counter")
    counter_file.write_binary(pickle.dumps(counter))

    def builder1(config):
        counter = pickle.loads(counter_file.read_binary())
        counter[0] += 1
        counter_file.write_binary(pickle.dumps(counter))
        return config * 10

    def builder2(config, deps):
        counter = pickle.loads(counter_file.read_binary())
        counter[1] += 1
        counter_file.write_binary(pickle.dumps(counter))
        return sum(e.value for e in deps)

    def make_deps(config):
        return [col1.ref(x) for x in range(config)]

    col1 = runtime.register_collection("col1", builder1)
    col2 = runtime.register_collection("col2", builder2, make_deps)

    e = col2.compute(5)

    counter = pickle.loads(counter_file.read_binary())
    assert counter == [5, 1]
    assert e.value == 100

    e = col2.compute(4)

    counter = pickle.loads(counter_file.read_binary())
    assert counter == [5, 2]
    assert e.value == 60

    col1.remove_many([0, 3])

    e = col2.compute(6)
    counter = pickle.loads(counter_file.read_binary())
    assert counter == [8, 3]
    assert e.value == 150

    e = col2.compute(6)
    counter = pickle.loads(counter_file.read_binary())
    assert counter == [8, 3]
    assert e.value == 150

    col2.remove(6)
    e = col2.compute(5)
    counter = pickle.loads(counter_file.read_binary())
    assert counter == [8, 4]
    assert e.value == 100

    e = col2.compute(6)
    counter = pickle.loads(counter_file.read_binary())
    assert counter == [8, 5]
    assert e.value == 150


def test_collection_double_ref(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_collection("col1", lambda c: c * 10)
    col2 = runtime.register_collection("col2",
                                       (lambda c, d: sum(x.value for x in d)),
                                       (lambda c: [col1.ref(10), col1.ref(10), col1.ref(10)]))
    assert col2.compute("abc").value == 300


def test_collection_stored_deps(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_collection("col1", lambda c: c * 10)
    col2 = runtime.register_collection("col2",
                                       (lambda c, d: sum(x.value for x in d)),
                                       lambda c: [col1.ref(i) for i in range(c["start"], c["end"], c["step"])])
    col3 = runtime.register_collection("col3",
                                       (lambda c, d: sum(x.value for x in d)),
                                       lambda c: [col2.ref({"start": 0, "end": c, "step": 2}), col2.ref({"start": 0, "end": c, "step": 3})])
    assert col3.compute(10).value == 380


    cc2_2 = {"end": 10, "start": 0, "step": 2}
    cc2_3 = {"end": 10, "start": 0, "step": 3}
    c2_2 = col2.make_key(cc2_2)
    c2_3 = col2.make_key(cc2_3)

    assert col3.get_entry_state(10) == "finished"
    assert col2.get_entry_state(cc2_2) == "finished"
    assert col2.get_entry_state(cc2_3) == "finished"
    assert col1.get_entry_state(0) == "finished"
    assert col1.get_entry_state(2) == "finished"


    assert set(runtime.db.get_recursive_consumers(col1.name, "2")) == {
        ("col1", "2"), ('col2', "{'end':10,'start':0,'step':2,}"), ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col1.name, "6")) == {
        ("col1", "6"), ('col2', c2_2), ("col2", c2_3),  ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col1.name, "9")) == {
            ("col1", "9"), ("col2", c2_3),  ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col2.name, c2_3)) == {
        ("col2", c2_3),  ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col3.name, col3.make_key(10))) == {
        ('col3', '10')
    }

    col1.remove(6)
    assert col3.get_entry_state(10) is  None
    assert col2.get_entry_state(cc2_2) is None
    assert col2.get_entry_state(cc2_3) is None
    assert col1.get_entry_state(0) == "finished"
    assert col1.get_entry_state(6) is None
    assert col1.get_entry_state(2) == "finished"

    col1.remove(0)
    assert col3.get_entry_state(10) is  None
    assert col2.get_entry_state(cc2_2) is None
    assert col2.get_entry_state(cc2_3) is None
    assert col1.get_entry_state(0) is None
    assert col1.get_entry_state(6) is None
    assert col1.get_entry_state(2) == "finished"

    assert col3.compute(10).value == 380

    assert col3.get_entry_state(10) == "finished"
    assert col2.get_entry_state(cc2_2) == "finished"
    assert col2.get_entry_state(cc2_3) == "finished"
    assert col1.get_entry_state(0) == "finished"
    assert col1.get_entry_state(2) == "finished"

    col1.remove(2)

    assert col3.get_entry_state(10) is  None
    assert col2.get_entry_state(cc2_2) is None
    assert col2.get_entry_state(cc2_3) == "finished"
    assert col1.get_entry_state(0) == "finished"
    assert col1.get_entry_state(6) == "finished"
    assert col1.get_entry_state(2) is None

    col1.compute(2)

    #runtime.serve()


def test_collection_clean(env, tmpdir):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    counter_file = tmpdir.join("counter")
    counter_file.write_binary(pickle.dumps(0))

    def fn(config):
        counter = pickle.loads(counter_file.read_binary())
        counter += 1
        counter_file.write_binary(pickle.dumps(counter))
        return config

    col1 = runtime.register_collection("col1", fn)
    res = col1.compute(1)
    assert pickle.loads(counter_file.read_binary()) == 1
    res = col1.compute(1)
    assert pickle.loads(counter_file.read_binary()) == 1

    col1.clean()
    res = col1.compute(1)
    assert pickle.loads(counter_file.read_binary()) == 2
