from orco.ref import _make_key_helper
from orco import computation


def key_tester(obj):
    stream = []
    _make_key_helper(obj, stream)
    return "".join(stream)


def test_ref_key_basics():
    assert key_tester(1) == "1"
    assert key_tester("abc") == "'abc'"
    assert key_tester([1, 2, 3]) == "[1,2,3,]"
    assert key_tester({"a": 10, 1: [20, 0]}) == "{'a':10,1:[20,0,],}"

    @computation()
    def my_comp(x, y):
        return 0

    ref = my_comp.ref(10, 20)
    assert key_tester(ref) == "<Ref my_comp,0,{'x': 10, 'y': 20},0>"

    assert (
        key_tester({ref: 10})
        == "{~ce9663737113a2436d87c33a9e7f54b6d831ddf91269072c66fd85a3:10,}"
    )
