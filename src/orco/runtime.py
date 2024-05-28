from contextvars import ContextVar
from typing import Union
from threading import Lock

from .database import Database
from .ref import collect_refs, replace_refs
from .globals import _REGISTERED_COMPUTATION

_GLOBAL_RUNTIME: ContextVar[Union[None, "Runtime"]] = ContextVar(
    "_GLOBAL_RUNTIME", default=None
)


class Runtime:
    """
    Core class of ORCO.

    It manages database with results and starts computations

    For SQLite:

    >>> runtime = Runtime("sqlite:///path/to/dbfile.db")

    For Postgress:

    >>> runtime = Runtime("postgresql://<USERNAME>:<PASSWORD>@<HOSTNAME>/<DATABASE>")
    """

    def __init__(self, db_path: str, n_processes=None):
        self.db = Database(db_path)
        self.db.init()
        self._token = None
        self.lock = Lock()

    def compute(self, obj):
        refs = collect_refs(obj)
        results = self.db.get_results(refs)
        for ref in refs:
            if ref not in results:
                call_result = _REGISTERED_COMPUTATION[ref.name].fn(**ref.config)
                self.db.insert(ref, call_result, {})
                results[ref] = call_result
        return replace_refs(obj, results)

    def __enter__(self):
        assert self._token is None
        self._token = _GLOBAL_RUNTIME.set(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        _GLOBAL_RUNTIME.reset(self._token)
        self._token = None


def get_current_runtime() -> Runtime:
    runtime = _GLOBAL_RUNTIME.get()
    if runtime is None:
        raise Exception("No running runtime")
    return runtime
