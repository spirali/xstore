
from collections import namedtuple
from datetime import datetime

from .obj import Obj
from .entry import Entry
from .task import Task
from .ref import Ref


def _default_make_key_helper(obj, stream):
    if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, float):
        stream.append(repr(obj))
    elif isinstance(obj, list) or isinstance(obj, tuple):
        stream.append("[")
        for value in obj:
            _default_make_key_helper(value, stream)
            stream.append(",")
        stream.append("]")
    elif isinstance(obj, dict):
        stream.append("{")
        for key, value in sorted(obj.items()):
            if not isinstance(key, str):
                raise Exception("Invalid key in config: '{}', type: {}".format(repr(key), type(key)))
            if key.startswith("_"):
                continue
            stream.append(repr(key))
            stream.append(":")
            _default_make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    else:
        raise Exception("Invalid item in config: '{}', type: {}".format(repr(obj), type(obj)))


def default_make_key(config):
    stream = []
    _default_make_key_helper(config, stream)
    return "".join(stream)


class Collection:

    def __init__(self, runtime, name: str, build_fn, dep_fn):
        self.runtime = runtime
        self.name = name
        self.build_fn = build_fn
        self.dep_fn = dep_fn

    def ref(self, config):
        return Ref(self, config)

    def compute(self, config):
        return self.compute_many([config])[0]

    def get_entry(self, config):
        entry = self.runtime.db.get_entry_no_config(self.name, self.make_key(config))
        entry.config = config
        return entry

    def has_entry(self, config):
        return self.runtime.db.has_entry_by_key(self.name, self.make_key(config))

    def get_entry_state(self, config):
        return self.runtime.db.get_entry_state(self.name, self.make_key(config))

    def remove(self, config):
        return self.runtime.db.remove_entry_by_key(self.name, self.make_key(config))

    def remove_many(self, configs):
        # TODO: Do in one step in DB
        for config in configs:
            self.remove(config)
        #return self.runtime.db.remove_entries(
        #    ((self.name, self.make_key(config)) for config in configs))

    def clean(self):
        self.runtime.db.clean_collection(self.name)

    def compute_many(self, configs):
        return self.runtime.compute_refs([self.ref(config) for config in configs])

    def insert(self, config, value):
        entry = Entry(config, value)
        self.runtime.db.create_entry(self.name, self.make_key(config), entry)

    def make_key(self, config):
        return default_make_key(config)
