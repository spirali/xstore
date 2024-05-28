from typing import Callable
import inspect

from .ref import Ref
from .runtime import get_current_runtime
from .globals import _REGISTERED_COMPUTATION


class Computation:
    def __init__(self, fn: Callable, name: str, version: int):
        assert isinstance(fn, Callable)
        self.fn = fn
        self.version = version
        self.fn_signature = inspect.signature(fn)
        self.fn_argspec = inspect.getfullargspec(fn)
        self.name = name or fn.__name__

    def __repr__(self):
        return f"<Computation '{self.name}'>"

    def ref(self, *args, version=None, replica=0, **kwargs):
        if version is None:
            version = self.version
        ba = self.fn_signature.bind(*args, **kwargs)
        ba.apply_defaults()
        a = ba.arguments
        if self.fn_argspec.varkw:
            kwargs = a.pop(self.fn_argspec.varkw, {})
            a.update(kwargs)
        return Ref(self.name, a, version, replica)

    def __call__(self, *args, **kwargs):
        runtime = get_current_runtime()
        ref = self.ref(*args, **kwargs)
        return runtime.compute(ref)


def computation(*, name: str = None, version: int = 0):
    def _helper(fn):
        c = Computation(fn, name, version)
        _REGISTERED_COMPUTATION[c.name] = c
        return c

    return _helper
