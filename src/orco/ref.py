from hashlib import sha224
from typing import Any


class Ref:
    def __init__(self, name: str, config: dict, version: int, replica: int):
        self.name = name
        self.config = config
        self.version = version
        self.replica = replica
        self.config_key = make_key(self.config)

    @property
    def tuple_key(self) -> tuple:
        return self.name, self.config_key, self.version, self.replica

    def __repr__(self):
        a = ", ".join(f"{k}={repr(v)}" for k, v in self.config.items())
        return f"<Ref {self.name}({a}) v={self.version} r={self.replica}>"

    def __eq__(self, other):
        return (
            self.config_key == other.config_key
            and self.name == other.name
            and self.version == other.version
            and self.replica == other.config_key
        )

    def __hash__(self):
        return hash((self.config_key, self.replica, self.version, self.name))


def collect_refs(obj) -> list[Ref]:
    result = []

    def _helper(obj2):
        if isinstance(obj2, Ref):
            result.append(obj2)
        if isinstance(obj2, list) or isinstance(obj2, tuple):
            for item in obj2:
                _helper(item)
        if isinstance(obj2, dict):
            for item in obj2.values():
                _helper(item)

    _helper(obj)
    return result


def replace_refs(obj, refs: dict[Ref, Any]):
    if isinstance(obj, Ref):
        return refs[obj]
    if isinstance(obj, list):
        return [replace_refs(item, refs) for item in obj]
    if isinstance(obj, tuple):
        return tuple(replace_refs(item, refs) for item in obj)
    if isinstance(obj, dict):
        return {key: replace_refs(value, refs) for key, value in obj.items()}
    return obj


def _make_key_helper(obj, stream):
    if (
        isinstance(obj, str)
        or isinstance(obj, int)
        or isinstance(obj, float)
        or obj is None
    ):
        stream.append(repr(obj))
    elif isinstance(obj, list) or isinstance(obj, tuple):
        stream.append("[")
        for value in obj:
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("]")
    elif isinstance(obj, dict):
        stream.append("{")
        for key, value in sorted(obj.items()):
            if not isinstance(key, str):
                raise Exception(
                    "Invalid key in config: '{}', type: {}".format(repr(key), type(key))
                )
            if key.startswith("__"):
                continue
            stream.append(repr(key))
            stream.append(":")
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    else:
        raise Exception(
            "Invalid item in config: '{}', type: {}".format(repr(obj), type(obj))
        )


def make_key(config):
    stream = []
    _make_key_helper(config, stream)
    return sha224("".join(stream).encode()).hexdigest()
