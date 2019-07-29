import multiprocessing
import threading
import time
from datetime import datetime

from .collection import Entry
from .task import Task


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

    def __init__(self, heartbeat_interval=5, n_processes=None):
        super().__init__("local", "0.0", "{} cpus".format(multiprocessing.cpu_count()),
                         heartbeat_interval)
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None
        self.cache = {}

        self.pool = None
        if n_processes is not None and n_processes > 1:
            self.pool = multiprocessing.Pool(n_processes)

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

    def save_result(self, task: Task, result):
        ref = task.ref
        collection = ref.collection
        entry = Entry(collection, ref.config, result, datetime.now())
        collection.runtime.db.set_entry_value(self.id, entry)
        self.stats["n_completed"] += 1
        collection.runtime.db.update_stats(self.id, self.stats)
        return entry

    def run_tasks(self, tasks):
        computed = {}
        missing = {}

        for task in tasks:
            result = self.cache.get(task)
            if result is None:
                inputs = None
                if task.inputs:
                    inputs = self.run_tasks(task.inputs)

                collection = task.ref.collection
                missing[task] = (collection.build_fn, task.ref.config,
                                 collection.dep_fn is not None, inputs)
            else:
                computed[task] = result

        def save(task, result):
            entry = self.save_result(task, result)
            self.cache[task] = entry
            computed[task] = entry

        def compute_local():
            for (task, args) in missing.items():
                save(task, compute_task(args))

        def compute_multiprocessing():
            results = self.pool.imap(compute_task, missing.values())

            for (task, result) in zip(missing.keys(), results):
                save(task, result)

        fn = compute_multiprocessing if self.pool else compute_local
        fn()

        result = []
        for task in tasks:
            result.append(computed[task])

        return result

    def run(self, all_tasks, required_tasks: [Task]):
        n_tasks = sum(1 for t in all_tasks.values() if not t.is_computed)
        self.stats = {
            "n_tasks": n_tasks,
            "n_completed": 0
        }
        return self.run_tasks(required_tasks)
