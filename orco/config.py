import itertools
import json
from collections import Iterable


def check_type_all(iterable, type):
    for item in iterable:
        if not isinstance(item, type):
            return False
    return True


def is_list_like(item):
    return isinstance(item, list) or isinstance(item, tuple)


def get_magic_operator(value):
    if not isinstance(value, dict):
        return None

    keys = list(value.keys())
    if len(keys) == 1 and isinstance(keys[0], str) and len(keys[0]) > 0 and keys[0][0] == "$":
        return keys[0]
    return None


def resolve_ref(data, computed, resolving, key):
    if key not in computed:
        assert key not in resolving

        resolving.add(key)
        computed[key] = resolve(data, computed, resolving, data[key])
        resolving.remove(key)
    return computed[key]


def resolve_range(data, computed, resolving, args):
    if isinstance(args, int):
        ret = range(args)
    elif is_list_like(args):
        assert 2 <= len(args) <= 3
        ret = range(*args)
    else:
        assert False
    return list(ret)


def resolve_concat(data, computed, resolving, args):
    assert isinstance(args, Iterable)
    items = [resolve(data, computed, resolving, item) for item in args]
    assert check_type_all(items, Iterable)
    return list(itertools.chain.from_iterable(items))


def resolve_product(data, computed, resolving, args):
    if is_list_like(args):
        args = [resolve(data, computed, resolving, item) for item in args]
        assert check_type_all(args, list) or check_type_all(args, tuple)
        return list(itertools.product(*args))
    elif isinstance(args, dict):
        values = [resolve(data, computed, resolving, item) for item in args.values()]
        assert check_type_all(values, Iterable)
        return [dict(zip(args.keys(), items)) for items in itertools.product(*values)]
    else:
        assert False


def resolve_zip(data, computed, resolving, args):
    assert is_list_like(args)
    assert check_type_all(args, list) or check_type_all(args, tuple)
    return list(zip(*[resolve(data, computed, resolving, item) for item in args]))


def resolve(data, computed, resolving, value):
    op = get_magic_operator(value)
    if op is not None:
        args = value[op]
        ops = {
            "$ref": resolve_ref,
            "$range": resolve_range,
            "$+": resolve_concat,
            "$product": resolve_product,
            "$zip": resolve_zip
        }
        assert op in ops
        return ops[op](data, computed, resolving, args)

    if is_list_like(value):
        return [resolve(data, computed, resolving, item) for item in value]
    elif isinstance(value, dict):
        return {key: resolve(data, computed, resolving, item) for (key, item) in value.items()}

    return value


def parse_config(data):
    assert isinstance(data, dict)

    computed = {}
    for (key, value) in data.items():
        if key not in computed:
            resolving = {key}
            computed[key] = resolve(data, computed, resolving, value)
    return computed


def parse_config_file(path: str):
    with open(path) as f:
        return parse_config(json.load(f))
