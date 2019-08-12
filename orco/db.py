import json
import logging
import math
import pickle
from concurrent.futures import ThreadPoolExecutor

import apsw
import pandas as pd

from .entry import Entry

logger = logging.getLogger(__name__)


class DB:

    DEAD_EXECUTOR_QUERY = "((STRFTIME('%s', heartbeat) + heartbeat_interval * 2) - STRFTIME('%s', 'now') < 0)"
    LIVE_EXECUTOR_QUERY = "((STRFTIME('%s', heartbeat) + heartbeat_interval * 2) - STRFTIME('%s', 'now') >= 0)"
    RECURSIVE_CONSUMERS = """
            WITH RECURSIVE
            selected(collection, key) AS (
                VALUES(?, ?)
                UNION
                SELECT collection_t,  cast(key_t as TEXT) FROM selected, deps WHERE selected.collection == deps.collection_s AND selected.key == deps.key_s
            )"""

    def __init__(self, path, threading=True):
        logger.debug("Opening DB: %s, threading = %s", path, threading)

        def _helper():
            self.conn = apsw.Connection(path)
            self.conn.setbusytimeout(5000)  # In milliseconds

            # The following HAS TO BE OUTSIDE OF TRANSACTION!
            self.conn.cursor().execute("""
                    PRAGMA foreign_keys = ON
                """)
        self.path = path
        if threading:
            self._thread = ThreadPoolExecutor(max_workers=1)
        else:
            self._thread = None
        self._run(_helper)

    def _run(self, fn):
        thread = self._thread
        if thread:
            return thread.submit(fn).result()
        else:
            return fn()

    def init(self):
        def _helper():
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS collections (
                        name TEXT NOT NULL PRIMARY KEY
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS executors (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        heartbeat TEXT NOT NULL,
                        heartbeat_interval FLOAT NOT NULL,
                        stats TEXT,
                        created TEXT NOT NULL,
                        type STRING NOT NULL,
                        version STRING NOT NULL,
                        resources STRING NOT NULL
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS entries (
                        collection STRING NOT NULL,
                        key TEXT NOT NULL,
                        executor INTEGER,
                        value BLOB,
                        config BLOB NOT NULL,
                        value_repr STRING,
                        created TEXT,
                        comp_time FLOAT,

                        PRIMARY KEY (collection, key)
                        CONSTRAINT collection_ref
                            FOREIGN KEY (collection)
                            REFERENCES collections(name)
                            ON DELETE CASCADE
                        CONSTRAINT executor_ref
                            FOREIGN KEY (executor)
                            REFERENCES executors(id)
                            ON DELETE CASCADE
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS deps (
                        collection_s STRING NOT NULL,
                        key_s STRING NOT NULL,
                        collection_t STRING NOT NULL,
                        key_t STRING NOT NULL,

                        UNIQUE(collection_s, key_s, collection_t, key_t),

                        CONSTRAINT entry_s_ref
                            FOREIGN KEY (collection_s, key_s)
                            REFERENCES entries(collection, key)
                            ON DELETE CASCADE,
                        CONSTRAINT entry_t_ref
                            FOREIGN KEY (collection_t, key_t)
                            REFERENCES entries(collection, key)
                            ON DELETE CASCADE
                    );
                """)
        self._run(_helper)

    def _dump(self):
        def _helper():
            print("ENTRIES ----")
            c = self.conn.cursor()
            r = c.execute("SELECT * FROM entries")
            for x in r:
                print(x)
            print("DEPS -----")
            c = self.conn.cursor()
            r = c.execute("SELECT * FROM deps")
            for x in r:
                print(x)
        self._run(_helper)

    def _ensure_collection_query(self, cursor, name):
        cursor.execute("INSERT OR IGNORE INTO collections VALUES (?)", [name])

    def ensure_collection(self, name):
        def _helper():
            with self.conn:
                c = self.conn.cursor()
                self._ensure_collection_query(c, name)
        self._run(_helper)

    def clean_collection(self, name):
        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute("""
WITH RECURSIVE
    selected(collection, key) AS (
        SELECT collection, key
        FROM entries
        WHERE collection = (?)
        UNION
        SELECT collection_t,  cast(key_t as TEXT)
        FROM selected
        JOIN deps ON selected.collection == deps.collection_s AND selected.key == deps.key_s
    )
DELETE FROM entries
WHERE rowid IN
    (SELECT entries.rowid
    FROM selected
    LEFT JOIN entries ON entries.collection == selected.collection AND entries.key == selected.key)
    """, [name])
        self._run(_helper)

    def create_entries(self, raw_entries):
        def _helper():
            data = [(e.collection_name, e.key, e.value, e.config,
                     e.value_repr) for e in raw_entries]
            with self.conn:
                c = self.conn.cursor()
                c.executemany("INSERT INTO entries VALUES (?, ?, null, ?, ?, ?, DATETIME('now'), null)", data)
        self._run(_helper)

    def set_entry_values(self, executor_id, raw_entries, stats=None):
        if stats is not None:
            stats_data = (json.dumps(stats), executor_id)
        else:
            stats_data = None

        def _helper():
            data = [
                (e.value, e.value_repr, e.comp_time, e.collection_name, e.key, executor_id)
                for e in raw_entries
            ]
            changes = 0
            with self.conn:
                c = self.conn.cursor()
                for d in data:
                    c.execute("UPDATE entries SET value = ?, value_repr = ?, created = DATETIME('now'), comp_time = ? WHERE collection = ? AND key = ? AND executor = ? AND value is null", d)
                    changes += self.conn.changes()
                if stats_data:
                    c.execute("""UPDATE executors SET stats = ?, heartbeat = DATETIME('now') WHERE id = ?""", stats_data)
            return changes
        if self._run(_helper) != len(raw_entries):
            raise Exception("Setting value to unannouced config (all configs: {})", ["{}/{}".format(c.collection_name, c.key) for c in raw_entries])

    def get_recursive_consumers(self, collection_name, key):
        #WHERE EXISTS(SELECT null FROM selected AS s WHERE deps.collection_s == selected.collection AND deps.key_s == selected.key
        query = """
            {}
            SELECT collection, key FROM selected
        """.format(self.RECURSIVE_CONSUMERS)
        def _helper():
            c = self.conn.cursor()
            rs = c.execute(query, [collection_name, key])
            return [(r[0], r[1]) for r in rs]
        return self._run(_helper)

    def has_entry_by_key(self, collection_name, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT COUNT(*) FROM entries WHERE collection = ? AND key = ? AND value is not null",
                      [collection_name, key])
            return bool(c.fetchone()[0])
        return self._run(_helper)

    def get_entry_state(self, collection_name, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT value is not null FROM entries WHERE collection = ? AND key = ? AND (value is not null OR executor is null OR executor in (SELECT id FROM executors WHERE {}))".format(self.LIVE_EXECUTOR_QUERY),
                      [collection_name, key])
            v = c.fetchone()
            if v is None:
                return None
            if v[0]:
                return "finished"
            else:
                return "announced"
        return self._run(_helper)

    def get_entry_no_config(self, collection_name, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT value, created FROM entries WHERE collection = ? AND key = ? AND (value is not null OR executor is null OR executor in (SELECT id FROM executors WHERE {}))".format(self.LIVE_EXECUTOR_QUERY),
                    [collection_name, key])
            return c.fetchone()
        result = self._run(_helper)
        if result is None:
            return None
        return Entry(None, pickle.loads(result[0]) if result[0] is not None else None, result[1])

    def get_entry(self, collection_name, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT config, value, created FROM entries WHERE collection = ? AND key = ?",
                    [collection_name, key])
            return c.fetchone()
        result = self._run(_helper)
        if result is None:
            return None
        config, value, created = result
        return Entry(pickle.loads(config), pickle.loads(value) if value is not None else None, created)

    def remove_entries_by_key(self, collection_name, keys):
        def _helper():
            with self.conn:
                self.conn.cursor().executemany("""{}
                DELETE FROM entries
                WHERE rowid IN
                    (SELECT entries.rowid
                     FROM selected LEFT JOIN entries ON
                     entries.collection == selected.collection AND entries.key == selected.key)
                """.format(self.RECURSIVE_CONSUMERS), [[collection_name, key] for key in keys])
        self._run(_helper)

    def invalidate_entries_by_key(self, collection_name, keys):
        def _helper():
            with self.conn:
                self.conn.cursor().executemany("""
WITH RECURSIVE
children(collection, key) AS (
    WITH RECURSIVE
    parents(collection, key) AS (
        VALUES(?, ?)
        UNION
        SELECT collection_s,  cast(key_s as TEXT) FROM parents, deps WHERE parents.collection == deps.collection_t AND parents.key == deps.key_t
    )
    SELECT *
    FROM parents
    UNION
    SELECT collection_t,  cast(key_t as TEXT) FROM children, deps WHERE children.collection == deps.collection_s AND children.key == deps.key_s
)
DELETE FROM entries
    WHERE rowid IN
        (SELECT entries.rowid
         FROM children LEFT JOIN entries ON
         entries.collection == children.collection AND entries.key == children.key)
                """.format(self.RECURSIVE_CONSUMERS), [[collection_name, key] for key in keys])
        self._run(_helper)

    """
    def remove_entries(self, collection_key_pairs):
        def _helper():
            self.conn.executemany("DELETE FROM entries WHERE collection = ? AND key = ?", collection_key_pairs)
        self.executor.submit(_helper).result()
    """

    def collection_summaries(self):
        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT collection, COUNT(key), TOTAL(length(value)), TOTAL(length(config)) FROM entries GROUP BY collection ORDER BY collection")
            result = []
            found = set()
            for name, count, size_value, size_config in r.fetchall():
                found.add(name)
                result.append({"name": name, "count": count, "size": size_value + size_config})

            c.execute("SELECT name FROM collections")
            for x in r.fetchall():
                name = x[0]
                if name in found:
                    continue
                result.append({"name": name, "count": 0, "size": 0})

            result.sort(key=lambda x: x["name"])
            return result
        return self._run(_helper)

    def _cleanup_lost_entries(self, cursor):
        cursor.execute("DELETE FROM entries WHERE value is null AND executor IN (SELECT id FROM executors WHERE {})".format(self.DEAD_EXECUTOR_QUERY))

    def announce_entries(self, executor_id, refs, deps):
        def _helper():
            entry_data = [(r.collection.name,
                           r.collection.make_key(r.config),
                           pickle.dumps(r.config),
                           executor_id) for r in refs]
            deps_data = [
                (r1.collection.name,
                 r1.collection.make_key(r1.config),
                 r2.collection.name,
                 r2.collection.make_key(r2.config)) for r1, r2 in deps
            ]
            try:
                with self.conn:
                    c = self.conn.cursor()
                    self._cleanup_lost_entries(c)
                    c.executemany("INSERT INTO entries(collection, key, config, executor) VALUES (?, ?, ?, ?)", entry_data)
                    c.executemany("INSERT INTO deps VALUES (?, ?, ?, ?)", deps_data)
                return True
            except apsw.ConstraintError as e:
                logger.debug(e)
                return False
        assert executor_id is not None
        return self._run(_helper)

    def unannounce_entries(self, executor_id, ref_keys):
        def _helper():
            data = [(r.collection, r.key, executor_id) for r in ref_keys]
            with self.conn:
                c = self.conn.cursor()
                self._cleanup_lost_entries(c)
                c.executemany("DELETE FROM entries WHERE collection = ? AND key = ? AND executor = ? AND value is null", data)
        return self._run(_helper)

    def entry_summaries(self, collection_name):
        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT key, config, length(value), value_repr, created, comp_time FROM entries WHERE collection = ?", [collection_name])
            return [
                {"key": key,
                 "config": pickle.loads(config),
                 "size": value_size + len(config) if value_size else len(config),
                 "value_repr": value_repr,
                 "created": created,
                 "comp_time": comp_time}
                for key, config, value_size, value_repr, created, comp_time in r.fetchall()
            ]
        return self._run(_helper)

    def register_executor(self, executor):
        assert executor.id is None
        def _helper():
            stats = json.dumps(executor.get_stats())
            with self.conn:
                c = self.conn.cursor()
                c.execute("INSERT INTO executors(created, heartbeat, heartbeat_interval, stats, type, version, resources) VALUES (?, DATETIME('now'), ?, ?, ?, ?, ?)",
                        [executor.created.isoformat(),
                        executor.heartbeat_interval,
                        stats,
                        executor.executor_type,
                        executor.version,
                        executor.resources])
                executor.id = self.conn.last_insert_rowid()
        self._run(_helper)

    def executor_summaries(self):
        def get_status(is_dead, stats):
            if stats is None:
                return "stopped"
            if not is_dead:
                return "running"
            else:
                return "lost"

        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT id, created, {}, stats, type, version, resources FROM executors".format(self.DEAD_EXECUTOR_QUERY))
            #r = c.execute("SELECT uuid, created, , stats, type, version, resources FROM executors")

            return [
                {"id": id,
                 "created": created,
                 "status": get_status(is_dead, stats),
                 "stats": json.loads(stats) if stats else None,
                 "type": executor_type,
                 "version": version,
                 "resources": resources,
                } for id, created, is_dead, stats, executor_type, version, resources in r.fetchall()
            ]
        return self._run(_helper)

    def get_run_stats(self, collection_name):
        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute("""SELECT AVG(comp_time), COUNT(comp_time) FROM entries WHERE collection = ? AND comp_time is not null""", [collection_name])
                avg, count = c.fetchone()
                if avg and count > 2:
                    c.execute("""SELECT SUM((comp_time - ?2) * (comp_time - ?2)) FROM entries WHERE collection = ?1 AND comp_time is not null""", [collection_name, avg])
                    stdev = math.sqrt(c.fetchone()[0] / (count - 1))
                elif avg:
                    stdev = 0
                else:
                    stdev = None
                return {"avg": avg, "stdev": stdev, "count": count}
        return self._run(_helper)

    def update_heartbeat(self, id):
        def _helper():
            id_list = [id]
            with self.conn:
                c = self.conn.cursor()
                c.execute("""UPDATE executors SET heartbeat = DATETIME('now') WHERE id = ? AND stats is not null""", id_list)
        self._run(_helper)

    def update_stats(self, id, stats):
        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute("""UPDATE executors SET stats = ?, heartbeat = DATETIME('now') WHERE id = ?""", stats_data)
        stats_data = [json.dumps(stats), id]
        self._run(_helper)

    def update_executor_stats(self, uuid, stats):
        assert stats != None
        raise NotImplementedError

    def stop_executor(self, id):
        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute("""UPDATE executors SET heartbeat = DATETIME('now'), stats = null WHERE id = ?""", [id])
                c.execute("""DELETE FROM entries WHERE executor == ? AND value is null""", [id])
        self._run(_helper)

    def to_pandas(self, collection_name):
        def _helper():
            c = self.conn.cursor()
            r = c.execute(
                "SELECT config, value, comp_time FROM entries WHERE collection = ?",
                [collection_name])
            return [
                {"config": pickle.loads(config),
                 "value": pickle.loads(value),
                 "comp_time": comp_time}
                for config, value, comp_time in r.fetchall()
            ]
        return pd.DataFrame(self._run(_helper))
