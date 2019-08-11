import pytest

from orco.config import parse_config


def test_config_simple():
    config = parse_config({
        "a": 5,
        "b": "hello",
        "c": ["hello", "world"],
        "d": {
            "key": {
                "orco": ["organized", "computing"]
            }
        }
    })
    assert config["a"] == 5
    assert config["b"] == "hello"
    assert config["c"] == ["hello", "world"]
    assert config["d"] == {
        "key": {
            "orco": ["organized", "computing"]
        }
    }


def test_config_ref():
    config = parse_config({
        "a": [{"$ref": "b"}, {"$ref": "c"}],
        "b": "hello",
        "c": ["hello", "world"],
    })
    assert config["a"] == ["hello", ["hello", "world"]]


def test_config_ref_cycle():
    with pytest.raises(AssertionError):
        parse_config({
            "a": [{"$ref": "b"}, {"$ref": "c"}],
            "b": {"$ref": "a"},
            "c": ["hello", "world"],
        })


def test_config_range():
    assert parse_config({
        "a": {"$range": 5}
    })["a"] == list(range(5))

    assert parse_config({
        "a": {"$range": [2, 5]}
    })["a"] == list(range(2, 5))

    assert parse_config({
        "a": {"$range": [3, 40, 5]}
    })["a"] == list(range(3, 40, 5))


def test_config_concat():
    assert parse_config({
        "a": {"$+": [[1, 2], [3, 4]]}
    })["a"] == [1, 2, 3, 4]

    assert parse_config({
        "a": {"$+": [{"$ref": "b"}, {"$ref": "c"}, {"$ref": "b"}, [4, 5]]},
        "b": [1, 2, 3],
        "c": [4, 5, 6]
    })["a"] == [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5]


def test_config_product():
    assert parse_config({
        "a": {"$product": [{"$range": 2}, [3, 4]]}
    })["a"] == [(0, 3), (0, 4), (1, 3), (1, 4)]

    assert parse_config({
        "b": 1,
        "a": {"$product": {
            "a": [{"$ref": "b"}, 2],
            "b": ["a", "b"],
            "c": [4, 5]
        }}
    })["a"] == [
               {"a": 1, "b": "a", "c": 4},
               {"a": 1, "b": "a", "c": 5},
               {"a": 1, "b": "b", "c": 4},
               {"a": 1, "b": "b", "c": 5},
               {"a": 2, "b": "a", "c": 4},
               {"a": 2, "b": "a", "c": 5},
               {"a": 2, "b": "b", "c": 4},
               {"a": 2, "b": "b", "c": 5},
           ]


def test_config_product_nested_unwrapped():
    assert parse_config({
        "a": {"$product": {
            "a": {"$product": {
                "x": [1, 2],
                "y": [3, 4]
            }},
            "b": ["a", "b"],
        }}
    })["a"] == [
               {'a': {'x': 1, 'y': 3}, 'b': 'a'},
               {'a': {'x': 1, 'y': 3}, 'b': 'b'},
               {'a': {'x': 1, 'y': 4}, 'b': 'a'},
               {'a': {'x': 1, 'y': 4}, 'b': 'b'},
               {'a': {'x': 2, 'y': 3}, 'b': 'a'},
               {'a': {'x': 2, 'y': 3}, 'b': 'b'},
               {'a': {'x': 2, 'y': 4}, 'b': 'a'},
               {'a': {'x': 2, 'y': 4}, 'b': 'b'}
           ]


def test_config_product_nested_wrapped():
    assert parse_config({
        "a": {"$product": {
            "a": [{"$product": {
                "x": [1, 2],
                "y": [3, 4]
            }}],
            "b": ["a", "b"],
        }}
    })["a"] == [
               {'a': [{'x': 1, 'y': 3}, {'x': 1, 'y': 4}, {'x': 2, 'y': 3}, {'x': 2, 'y': 4}],
                'b': 'a'},
               {'a': [{'x': 1, 'y': 3}, {'x': 1, 'y': 4}, {'x': 2, 'y': 3}, {'x': 2, 'y': 4}],
                'b': 'b'}
           ]


def test_config_zip():
    assert parse_config({
        "a": {"$product": {
            "a": {"$zip": [
                ["a", "b", "c"],
                [1, 2, 3]
            ]},
            "b": ["a", "b"],
        }}
    })["a"] == [{'a': ('a', 1), 'b': 'a'},
                {'a': ('a', 1), 'b': 'b'},
                {'a': ('b', 2), 'b': 'a'},
                {'a': ('b', 2), 'b': 'b'},
                {'a': ('c', 3), 'b': 'a'},
                {'a': ('c', 3), 'b': 'b'}]
