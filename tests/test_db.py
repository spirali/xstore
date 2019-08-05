import time

import pytest

from orco import LocalExecutor
from orco.entry import Entry


def test_db_announce(env):
    r = env.test_runtime()
    e1 = LocalExecutor(heartbeat_interval=1)
    e1._debug_do_not_start_heartbeat = True
    r.register_executor(e1)
    e2 = LocalExecutor(heartbeat_interval=1)
    r.register_executor(e2)

    c = r.register_collection("col1")
    assert r.db.announce_entries(e1.id, [c.ref("test1"), c.ref("test2")], [])
    assert not r.db.announce_entries(e2.id, [c.ref("test2"), c.ref("test3")], [])
    assert not r.db.announce_entries(e1.id, [c.ref("test2"), c.ref("test3")], [])
    assert r.db.announce_entries(e2.id, [c.ref("test3")], [])
    assert r.db.get_entry_state(c.name, c.make_key("test1")) == "announced"
    time.sleep(3)
    assert r.db.get_entry_state(c.name, c.make_key("test1")) is None
    assert not r.db.announce_entries(e2.id, [c.ref("test2"), c.ref("test3")], [])
    assert r.db.announce_entries(e2.id, [c.ref("test2")], [])
    assert not r.db.announce_entries(e2.id, [c.ref("test2")], [])


def test_db_set_value(env):
    r = env.test_runtime()
    e1 = LocalExecutor(heartbeat_interval=1)
    r.register_executor(e1)

    c = r.register_collection("col1")
    assert r.db.get_entry_state(c.name, c.make_key("cfg1")) is None

    r.db.announce_entries(e1.id, [c.ref("cfg1")], [])
    assert r.db.get_entry_state(c.name, c.make_key("cfg1")) == "announced"

    entry = Entry("cfg1", "value1")
    r.db.set_entry_value(e1.id, c.name, c.make_key(entry.config), entry)
    assert r.db.get_entry_state(c.name, c.make_key("cfg1")) == "finished"

    with pytest.raises(Exception):
        r.db.set_entry_value(e1.id, c.name, c.make_key(entry.config), entry)

    entry2 = Entry("cfg2", "value2")
    with pytest.raises(Exception):
        r.db.set_entry_value(e1.id, c.name, c.make_key(entry2.config), entry2)
    r.db.announce_entries(e1.id, [c.ref("cfg2")], [])
    r.db.set_entry_value(e1.id, c.name, c.make_key(entry2.config), entry2)

    with pytest.raises(Exception):
        r.db.create_entry(c.name, c.make_key(entry2.config), entry2)

    entry3 = Entry("cfg3", "value3")
    r.db.create_entry(c.name, c.make_key(entry3.config), entry3)


def test_db_run_stats(env):
    runtime = env.test_runtime()
    e1 = LocalExecutor(heartbeat_interval=1)
    runtime.register_executor(e1)
    c = runtime.register_collection("col1")
    c2 = runtime.register_collection("col2")

    assert runtime.db.announce_entries(e1.id, [c.ref("a"), c.ref("b"), c.ref("c"), c.ref("d"), c.ref("e")], [])
    assert runtime.db.get_entry_state(c.name, c.make_key("a")) == "announced"
    runtime.db._dump()
    entry = Entry("a", "value", comp_time=1)
    runtime.db.set_entry_value(e1.id, c.name, c.make_key("a"), entry)
    entry = Entry("b", "value", comp_time=2)
    runtime.db.set_entry_value(e1.id, c.name, c.make_key("b"), entry)
    entry = Entry("c", "value", comp_time=3)
    runtime.db.set_entry_value(e1.id, c.name, c.make_key("c"), entry)
    entry = Entry("d", "value", comp_time=4)
    runtime.db.set_entry_value(e1.id, c.name, c.make_key("d"), entry)

    r = runtime.db.get_run_stats("col1")
    assert pytest.approx(2.5) == r["avg"]
    assert pytest.approx(1.29099, 0.00001) == r["stdev"]
    assert r["count"] == 4

    r = runtime.db.get_run_stats("col2")
    assert None is r["avg"]
    assert None is r["stdev"]
    assert r["count"] == 0
