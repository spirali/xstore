from collections import Iterable


class Ref:
    __slots__ = ["collection", "config"]

    def __init__(self, collection, config):
        self.collection = collection
        self.config = config

    def ref_key(self):
        return RefKey(self.collection.name, self.collection.make_key(self.config))

    def __repr__(self):
        return "<{}/{}>".format(self.collection.name, repr(self.config))

    def __eq__(self, other):
        if not isinstance(other, Ref) or self.collection != other.collection:
            return False
        collection = self.collection
        return collection.make_key(self.config) == collection.make_key(other.config)

    def __hash__(self):
        return hash((self.collection, self.collection.make_key(self.config)))


class RefKey:
    __slots__ = ["collection", "key"]

    def __init__(self, collection, key):
        self.collection = collection
        self.key = key

    def __eq__(self, other):
        if not isinstance(other, RefKey):
            return False
        return (self.collection, self.key) == (other.collection, other.key)

    def __hash__(self):
        return hash((self.collection, self.key))


def collect_refs(dep_value, ref_set):
    if isinstance(dep_value, Ref):
        ref_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            collect_refs(val, ref_set)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            collect_refs(val, ref_set)


def resolve_refs(dep_value, ref_map):
    return walk_map(dep_value, RefKey, lambda r: ref_map[r])


def ref_to_refkey(dep_value):
    return walk_map(dep_value, Ref, lambda r: r.ref_key())


def walk_map(value, target_type, final_fn):
    if value is None:
        return None
    elif isinstance(value, target_type):
        return final_fn(value)
    elif isinstance(value, dict):
        return {
            key: walk_map(v, target_type, final_fn) for (key, v) in value.items()
        }
    elif isinstance(value, Iterable):
        return [walk_map(v, target_type, final_fn) for v in value]
    else:
        return value
