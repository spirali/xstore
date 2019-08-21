from collections import Iterable, namedtuple


class Ref:

    __slots__ = ["collection_name", "config", "key"]

    def __init__(self, collection_name, config):
        self.collection_name = collection_name
        self.config = config
        self.key = make_key(config)

    def __eq__(self, other):
        if not isinstance(other, Ref):
            return False
        if self.key != other.key:
            return False
        return self.collection_name == other.collection_name

    def __hash__(self):
        return hash((self.collection_name, self.key))

    def __repr__(self):
        return "<{}/{}>".format(self.collection_name, repr(self.config))

    def ref_key(self):
        return RefKey(self.collection_name, self.key)


def _make_key_helper(obj, stream):
    if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, float):
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
                raise Exception("Invalid key in config: '{}', type: {}".format(
                    repr(key), type(key)))
            if key.startswith("_"):
                continue
            stream.append(repr(key))
            stream.append(":")
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    else:
        raise Exception("Invalid item in config: '{}', type: {}".format(repr(obj), type(obj)))


def make_key(config):
    stream = []
    _make_key_helper(config, stream)
    return "".join(stream)


RefKey = namedtuple("RefKey", ("collection_name", "key"))


def collect_refs(obj):
    result = set()
    _collect_refs_helper(obj, result, Ref)
    return result


def _collect_refs_helper(dep_value, ref_set, r_class):
    if isinstance(dep_value, r_class):
        ref_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            _collect_refs_helper(val, ref_set, r_class)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            _collect_refs_helper(val, ref_set, r_class)


def collect_ref_keys(obj):
    result = set()
    _collect_refs_helper(obj, result, RefKey)
    return result


def _collect_ref_keys_helper(dep_value, ref_set):
    if isinstance(dep_value, Ref):
        ref_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            _collect_refs_helper(val, ref_set)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            _collect_refs_helper(val, ref_set)


def resolve_ref_keys(dep_value, ref_map):
    return walk_map(dep_value, RefKey, lambda r: ref_map[r])


def resolve_refs(dep_value, ref_map):
    return walk_map(dep_value, Ref, lambda r: ref_map[r])


def ref_to_refkey(dep_value):
    return walk_map(dep_value, Ref, lambda r: r.ref_key())


def walk_map(value, target_type, final_fn):
    if value is None:
        return None
    elif isinstance(value, target_type):
        return final_fn(value)
    elif isinstance(value, dict):
        return {key: walk_map(v, target_type, final_fn) for (key, v) in value.items()}
    elif isinstance(value, Iterable):
        return [walk_map(v, target_type, final_fn) for v in value]
    else:
        return value
