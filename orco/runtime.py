import collections
import logging
import threading
import time

from orco.ref import collect_refs
from .collection import Collection
from .db import DB
from .executor import Executor, Task
from .utils import format_time

logger = logging.getLogger(__name__)


class Runtime:

    def __init__(self, db_path, executor: Executor=None):
        self.db = DB(db_path)
        self.db.init()

        self._executor = executor
        self._collections = {}
        self._lock = threading.Lock()

        self.executors = []
        self.stopped = False

        logging.debug("Starting runtime %s (db=%s)", self, db_path)

        if executor:
            self.register_executor(executor)

    def __enter__(self):
        self._check_stopped()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.stopped:
            self.stop()

    def stop(self):
        self._check_stopped()

        logger.debug("Stopping runtime %s", self)
        for executor in self.executors:
            logger.debug("Stopping executor %s", executor)
            executor.stop()
        self.stopped = True

    def register_executor(self, executor):
        logger.debug("Registering executor %s", executor)
        executor.runtime = self
        self.db.register_executor(executor)
        executor.start()
        self.executors.append(executor)

    def unregister_executor(self, executor):
        logger.debug("Unregistering executor %s", executor)
        self.executors.remove(executor)
        self.db.stop_executor(executor.id)

    def register_collection(self, name, build_fn=None, dep_fn=None):
        with self._lock:
            if name in self._collections:
                raise Exception("Collection already registered")
            self.db.ensure_collection(name)
            collection = Collection(self, name, build_fn=build_fn, dep_fn=dep_fn)
            self._collections[name] = collection
            return collection

    @property
    def collections(self):
        with self._lock:
            return self._collections.copy()

    def collection_summaries(self):
        return self.db.collection_summaries()

    def entry_summaries(self, collection_name):
        return self.db.entry_summaries(collection_name)

    def executor_summaries(self):
        return self.db.executor_summaries()

    def update_heartbeat(self, id):
        self.db.update_heartbeat(id)

    def serve(self, port=8550, debug=False, testing=False, nonblocking=False):
        from .browser import init_service
        app = init_service(self)
        if testing:
            app.testing = True
            return app
        else:
            def run_app():
                app.run(port=port, debug=debug, use_reloader=False)

            if nonblocking:
                t = threading.Thread(target=run_app)
                t.daemon = True
                t.start()
                return t
            else:
                run_app()

    def get_entry(self, ref):
        collection = ref.collection
        entry = self.db.get_entry_no_config(collection.name, collection.make_key(ref.config))
        entry.config = ref.config
        return entry

    def _create_compute_tree(self, refs, exists):
        tasks = {}
        global_deps = set()
        conflicts = set()

        def make_task(ref):
            ref_key = ref.ref_key()
            if ref_key in exists:
                return ref
            if ref_key in conflicts:
                return None
            task = tasks.get(ref_key)
            if task is not None:
                return task
            collection = ref.collection
            state = self.db.get_entry_state(collection.name, ref_key.key)
            if state == "finished":
                exists.add(ref_key)
                return ref
            if state == "announced":
                conflicts.add(ref_key)
                return None
            if state is None and collection.dep_fn:
                deps = collection.dep_fn(ref.config)
                ref_set = set()
                collect_refs(deps, ref_set)
                inputs = [make_task(r) for r in ref_set]
                if any(inp is None for inp in inputs):
                    return None
                for r in ref_set:
                    global_deps.add((r, ref))
            else:
                inputs = None
            if state is None and collection.build_fn is None:
                raise Exception("Computation depends on a missing configuration '{}' in a fixed collection".format(ref))
            task = Task(ref, inputs)
            tasks[ref_key] = task
            return task

        for ref in refs:
            make_task(ref)

        return tasks, global_deps, len(conflicts)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _print_report(self, tasks):
        tasks_per_collection = collections.Counter([t.ref.collection.name for t in tasks.values()])
        print("Scheduled tasks  |     # | Expected comp. time (per entry)\n"
              "-----------------+-------+--------------------------------")
        for col, count in sorted(tasks_per_collection.items()):
            stats = self.db.get_run_stats(col)
            if stats["avg"] is None:
                print("{:<17}| {:>5} | N/A".format(col, count))
            else:
                print("{:<17}| {:>5} | {:>8} +- {}".format(
                    col, count, format_time(stats["avg"]), format_time(stats["stdev"])))
        print("-----------------+-------+--------------------------------")

    def _run_computation(self, refs, executor, exists):
        tasks, global_deps, n_conflicts = self._create_compute_tree(refs, exists)
        if not tasks and n_conflicts == 0:
            return "finished"
        need_to_compute_refs = [task.ref for task in tasks.values()]
        if not need_to_compute_refs:
            print("Waiting for finished computation on another executor ...")
            return "wait"
        logger.debug("Announcing refs %s at worker %s", need_to_compute_refs, executor.id)
        if not self.db.announce_entries(executor.id, need_to_compute_refs, global_deps):
            return "wait"
        try:
            del global_deps, need_to_compute_refs  # we do not this anymore, and .run may be long
            if n_conflicts:
                print("Some computation was temporarily skipped as they depends on tasks computed by another executor")
            self._print_report(tasks)
            executor.run(tasks)
            if n_conflicts == 0:
                return "finished"
            else:
                return "next"
        except:
            self.db.unannounce_entries(executor.id, list(tasks))
            raise

    def compute_refs(self, refs, executor=None):
        exists = set()
        if executor is None:
            if len(self.executors) == 0:
                raise Exception("No executors registered")
            executor = self.executors[0]

        while True:
            status = self._run_computation(refs, executor, exists)
            if status == "finished":
                break
            elif status == "next":
                continue
            elif status == "wait":
                time.sleep(1)
                continue
            else:
                assert 0
        return [self.get_entry(ref) for ref in refs]
