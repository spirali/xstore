from contextvars import ContextVar
from typing import Union, Any
from threading import Lock
from dataclasses import dataclass, field

from .database import Database
from .entry import AnnounceResult, EntryId, Entry
from .ref import collect_refs, replace_refs, Ref
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

    def read_entries(self, obj):
        refs = collect_refs(obj)
        results = {}
        for ref in refs:
            if ref in results:
                continue
            results[ref] = self.db.read_entry(ref)
        return replace_refs(obj, results)

    def read_results(self, obj):
        refs = collect_refs(obj)
        results = {}
        for ref in refs:
            if ref in results:
                continue
            results[ref] = self.db.read_result(ref)
        return replace_refs(obj, results)

    def read_refs(self, name: str) -> list[Entry]:
        return self.db.read_refs(name)

    def read_all_refs(self) -> list[Entry]:
        return self.db.read_all_refs()

    def _process_refs(self, refs) -> dict[Ref, Any]:
        results = {}
        current_running_task = _CURRENT_RUNNING_TASK.get()
        for ref in refs:
            if ref in results:
                continue
            status, entry_id, result = self.db.get_or_announce_entry(ref)
            if current_running_task is not None:
                current_running_task.deps.add(entry_id)
            if status == AnnounceResult.FINISHED:
                results[ref] = Entry(entry_id, ref, result)
            elif status == AnnounceResult.COMPUTE_HERE:
                try:
                    if ref.ephemeral_config:
                        config = ref.config.copy()
                        config.update(ref.ephemeral_config)
                    else:
                        config = ref.config
                    running_task = RunningTask()
                    token = _CURRENT_RUNNING_TASK.set(running_task)
                    result = _REGISTERED_COMPUTATION[ref.name].fn(**config)
                    _CURRENT_RUNNING_TASK.reset(token)
                except Exception as e:
                    self.db.cancel_entry(entry_id)
                    raise e
                self.db.finish_entry(entry_id, result, {}, running_task.deps)
                results[ref] = Entry(entry_id, ref, result)
            elif status == AnnounceResult.COMPUTING_ELSEWHERE:
                raise Exception(f"Computation {ref} is computed in another process")
        return results

    def get_entries(self, obj):
        refs = collect_refs(obj)
        return replace_refs(obj, self._process_refs(refs))

    def get_results(self, obj):
        refs = collect_refs(obj)
        results = {ref: entry.result for ref, entry in self._process_refs(refs).items()}
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


def get_results(obj):
    return get_current_runtime().get_results(obj)


def read_results(obj):
    return get_current_runtime().read_results(obj)
