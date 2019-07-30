from orco import LocalExecutor
from orco.entry import Entry
import time
from datetime import datetime

import pytest

def test_db_announce(env):
    r = env.runtime_in_memory()
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
    assert r.db.get_entry_state(c, c.make_key("test1")) == "announced"
    time.sleep(3)
    assert r.db.get_entry_state(c, c.make_key("test1")) is None
    assert not r.db.announce_entries(e2.id, [c.ref("test2"), c.ref("test3")], [])
    assert r.db.announce_entries(e2.id, [c.ref("test2")], [])
    assert not r.db.announce_entries(e2.id, [c.ref("test2")], [])


def test_db_set_value(env):
    r = env.runtime_in_memory()
    e1 = LocalExecutor(heartbeat_interval=1)
    r.register_executor(e1)

    c = r.register_collection("col1")
    assert r.db.get_entry_state(c, c.make_key("cfg1")) is None

    r.db.announce_entries(e1.id, [c.ref("cfg1")], [])
    assert r.db.get_entry_state(c, c.make_key("cfg1")) == "announced"

    entry = Entry("cfg1", "value1", datetime.now())
    r.db.set_entry_value(e1.id, c, entry)
    assert r.db.get_entry_state(c, c.make_key("cfg1")) == "finished"

    with pytest.raises(Exception):
        r.db.set_entry_value(e1.id, c, entry)

    entry2 = Entry("cfg2", "value2", datetime.now())
    with pytest.raises(Exception):
        r.db.set_entry_value(e1.id, c, entry2)
    r.db.announce_entries(e1.id, [c.ref("cfg2")], [])
    r.db.set_entry_value(e1.id, c, entry2)

    with pytest.raises(Exception):
        r.db.create_entry(c, entry2)

    entry3 = Entry("cfg3", "value3", datetime.now())
    r.db.create_entry(c, entry3)
