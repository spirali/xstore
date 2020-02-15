
from .entry import Entry
from .internals.key import make_key
from orco.internals.context import _CONTEXT


class Builder:
    """
    Builder - a factory for a task (a pair of builder and config)
    """

    def __init__(self, name: str):
        assert isinstance(name, str)
        self.name = name

    def __call__(self, config):
        entry = Entry(self.name, make_key(config), config, None, None, None)
        on_entry = _CONTEXT.on_entry
        if on_entry:
            on_entry(entry)
        return entry
