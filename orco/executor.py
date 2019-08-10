import logging
import multiprocessing
import threading
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime

import cloudpickle
import tqdm

from orco.ref import resolve_refs, ref_to_refkey, RefKey
from .db import DB
from .task import Task

logger = logging.getLogger(__name__)


class Executor:

    def __init__(self, executor_type, version, resources, heartbeat_interval):
        self.executor_type = executor_type
        self.version = version
        self.created = datetime.now()
        self.id = None
        self.runtime = None
        self.resources = resources
        self.stats = {}
        assert heartbeat_interval >= 1
        self.heartbeat_interval = heartbeat_interval

    def get_stats(self):
        raise NotImplementedError

    def run(self, tasks: [Task]):
        raise NotImplementedError

    def start(self):
        pass

    def stop(self):
        pass


def heartbeat(runtime, id, event, heartbeat_interval):
    while not event.is_set():
        runtime.update_heartbeat(id)
        time.sleep(heartbeat_interval)


def gather_announcements(tasks):
    result = set()


def compute_task(args):
    build_fn, config, has_input, input_entries = args

    if has_input:
        return build_fn(config, input_entries)
    else:
        return build_fn(config)


class LocalExecutor(Executor):

    _debug_do_not_start_heartbeat = False

    def __init__(self, heartbeat_interval=7, n_processes=None):
        super().__init__("local", "0.0", "{} cpus".format(multiprocessing.cpu_count()),
                         heartbeat_interval)
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

        self.pool = None
        self.n_processes = n_processes

    def get_stats(self):
        return self.stats

    def stop(self):
        if self.heartbeat_stop_event:
            self.heartbeat_stop_event.set()
        self.runtime.unregister_executor(self)
        self.runtime = None

    def start(self):
        assert self.runtime
        assert self.id is not None

        if not self._debug_do_not_start_heartbeat:
            self.heartbeat_stop_event = threading.Event()
            self.heartbeat_thread = threading.Thread(target=heartbeat,
                                                     args=(self.runtime, self.id,
                                                           self.heartbeat_stop_event,
                                                           self.heartbeat_interval))
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()

        self.pool = ProcessPoolExecutor(max_workers=self.n_processes)

    def _init(self, tasks):
        consumers = {}
        waiting_deps = {}
        ready = []

        for task in tasks:
            count = 0
            if task.inputs is not None:
                for inp in task.inputs:
                    if isinstance(inp, Task):
                        count += 1
                        c = consumers.get(inp)
                        if c is None:
                            c = []
                            consumers[inp] = c
                        c.append(task)
            if count == 0:
                ready.append(task)
            waiting_deps[task] = count
        return consumers, waiting_deps, ready

    def run(self, all_tasks):
        def process_unprocessed():
            logging.debug("Writing into db: %s", unprocessed)
            db.set_entry_values(self.id, unprocessed, self.stats)
            for raw_entry in unprocessed:
                ref_key = RefKey(raw_entry.collection_name, raw_entry.key)
                task = all_tasks[ref_key]
                #col_progressbars[ref_key[0]].update()
                for c in consumers.get(task, ()):
                    waiting_deps[c] -= 1
                    w = waiting_deps[c]
                    if w <= 0:
                        assert w == 0
                        waiting.add(submit(c))

        def submit(task):
            collection = task.ref.collection
            pickled_fns = pickle_cache.get(collection.name)
            if pickled_fns is None:
                pickled_fns = cloudpickle.dumps((collection.build_fn, collection._make_raw_entry))
                pickle_cache[collection.name] = pickled_fns
            dep_value = None
            if task.inputs is not None:
                dep_value = collection.dep_fn(task.ref.config)
                dep_value = ref_to_refkey(dep_value)
                inputs = [t.ref.ref_key() if isinstance(t, Task) else t.ref_key() for t in task.inputs]
            else:
                inputs = None
            return pool.submit(_run_task,
                               self.id,
                               db.path,
                               pickled_fns,
                               task.ref.ref_key(),
                               task.ref.config,
                               dep_value,
                               inputs)
        self.stats = {
            "n_tasks": len(all_tasks),
            "n_completed": 0
        }

        pickle_cache = {}
        pool = self.pool
        db = self.runtime.db
        db.update_stats(self.id, self.stats)
        consumers, waiting_deps, ready = self._init(all_tasks.values())
        waiting = [submit(task) for task in ready]
        del ready

        #col_progressbars = {}
        #for i, (col, count) in enumerate(tasks_per_collection.items()):
        #    col_progressbars[col] = tqdm.tqdm(desc=col, total=count, position=i)

        progressbar = tqdm.tqdm(total=len(all_tasks)) #  , position=i+1)
        unprocessed = []
        last_write = time.time()
        try:
            while waiting:
                wait_result = wait(waiting, return_when=FIRST_COMPLETED, timeout=1 if unprocessed else None)
                waiting = wait_result.not_done
                for f in wait_result.done:
                    self.stats["n_completed"] += 1
                    progressbar.update()
                    raw_entry = f.result()
                    logger.debug("Task finished: %s/%s", raw_entry.collection_name, raw_entry.key)
                    unprocessed.append(raw_entry)
                if unprocessed and (not waiting or time.time() - last_write > 1):
                    process_unprocessed()
                    unprocessed = []
                    last_write = time.time()
            #    db.update_stats(self.id, self.stats)
            #for p in col_progressbars.values():
            #    p.close()
            db.set_entry_values(self.id, unprocessed, self.stats)
        finally:
            progressbar.close()
            for f in waiting:
                f.cancel()


_per_process_db = None


def _run_task(executor_id, db_path, fns, ref_key, config, dep_value, deps):
    global _per_process_db
    if _per_process_db is None:
        _per_process_db = DB(db_path, threading=False)
    build_fn, finalize_fn = cloudpickle.loads(fns)

    start_time = time.time()

    if deps is not None:
        ref_map = {ref: _per_process_db.get_entry(ref.collection, ref.key) for ref in deps}
        dep_value = resolve_refs(dep_value, ref_map)
        value = build_fn(config, dep_value)
    else:
        value = build_fn(config)
    end_time = time.time()
    return finalize_fn(ref_key.collection, ref_key.key, None, value, end_time - start_time)
