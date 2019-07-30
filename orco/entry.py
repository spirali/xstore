

class Entry:

    __slots__ = ("config", "value", "created")

    def __init__(self, config, value, created):
        self.config = config
        self.value = value
        self.created = created

    @property
    def value_repr(self):
        value_repr = repr(self.value)
        if len(value_repr) > 85:
            value_repr = value_repr[:85] + " ..."
        return value_repr

    @property
    def is_computed(self):
        return bool(self.created)

    def __repr__(self):
        return "{}: {}".format(self.config, self.value_repr)
