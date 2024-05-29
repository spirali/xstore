from contextvars import ContextVar
from typing import Union
from threading import Lock
from dataclasses import dataclass, field

from .database import Database
from .entry import AnnounceResult, EntryId
from .ref import collect_refs, replace_refs
from .globals import _REGISTERED_COMPUTATION

_GLOBAL_RUNTIME: ContextVar[Union[None, "Runtime"]] = ContextVar(
    "_GLOBAL_RUNTIME", default=None
)


@dataclass
class RunningTask:
    deps: set[EntryId] = field(default_factory=set)


_CURRENT_RUNNING_TASK: ContextVar[Union[None, RunningTask]] = ContextVar(
    "_CURRENT_RUNNING_TASK", default=None
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

    def get(self, obj):
        refs = collect_refs(obj)
        results = {}
        for ref in refs:
            if ref in results:
                continue
            results[ref] = self.db.get_result(ref)
        return replace_refs(obj, results)

    def compute(self, obj):
        refs = collect_refs(obj)
        results = {}
        current_running_task = _CURRENT_RUNNING_TASK.get()
        for ref in refs:
            if ref in results:
                continue
            status, entry_id, result = self.db.get_or_announce_entry(ref)
            if current_running_task is not None:
                current_running_task.deps.add(entry_id)
            if status == AnnounceResult.FINISHED:
                results[ref] = result
            elif status == AnnounceResult.COMPUTE_HERE:
                try:
                    running_task = RunningTask()
                    token = _CURRENT_RUNNING_TASK.set(running_task)
                    result = _REGISTERED_COMPUTATION[ref.name].fn(**ref.config)
                    _CURRENT_RUNNING_TASK.reset(token)
                except Exception as e:
                    self.db.cancel_entry(entry_id)
                    raise e
                self.db.finish_entry(entry_id, result, {}, running_task.deps)
                results[ref] = result
            elif status == AnnounceResult.COMPUTING_ELSEWHERE:
                raise Exception(f"Computation {ref} is computed in another process")
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
