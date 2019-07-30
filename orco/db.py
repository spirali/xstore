import sqlite3
import pickle
import json
from concurrent.futures import ThreadPoolExecutor


from .entry import Entry


class DB:

    DEAD_EXECUTOR_QUERY = "((STRFTIME('%s', heartbeat) + heartbeat_interval * 2) - STRFTIME('%s', 'now') < 0)"
    LIVE_EXECUTOR_QUERY = "((STRFTIME('%s', heartbeat) + heartbeat_interval * 2) - STRFTIME('%s', 'now') >= 0)"

    def __init__(self, path):
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.init, path).result()
        assert self.conn is not None

    def init(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                name TEXT NOT NULL PRIMARY KEY
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS executors (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                created TEXT NOT NULL,
                heartbeat TEXT NOT NULL,
                heartbeat_interval FLOAT NOT NULL,
                stats TEXT,
                type STRING NOT NULL,
                version STRING NOT NULL,
                resources STRING NOT NULL
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                collection STRING NOT NULL,
                key TEXT NOT NULL,
                config BLOB NOT NULL,
                value BLOB,
                value_repr STRING,
                created TEXT,

                executor INTEGER,

                PRIMARY KEY (collection, key)
                CONSTRAINT collection_ref
                    FOREIGN KEY (collection)
                    REFERENCES collections(name)
                    ON DELETE CASCADE
                CONSTRAINT executor_ref
                    FOREIGN KEY (executor)
                    REFERENCES executors(uuid)
                    ON DELETE CASCADE
            );
        """)

        self.conn.execute("""
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

    def ensure_collection(self, name):
        def _helper():
            c = self.conn.cursor()
            c.execute("INSERT OR IGNORE INTO collections VALUES (?)", [name])
            self.conn.commit()
        self.executor.submit(_helper).result()

    def create_entry(self, collection, entry):
        def _helper():
            c = self.conn.cursor()
            c.execute("INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, null)",
                    [collection.name,
                    collection.make_key(entry.config),
                    pickle.dumps(entry.config),
                    pickle.dumps(entry.value),
                    entry.value_repr,
                    entry.created])
            self.conn.commit()
        self.executor.submit(_helper).result()

    def set_entry_value(self, executor_id, collection, entry):
        def _helper():
            c = self.conn.cursor()
            c.execute("UPDATE entries SET value = ?, value_repr = ?, created = ? WHERE collection = ? AND key = ? AND executor = ? AND value is null",
                    [pickle.dumps(entry.value),
                     entry.value_repr,
                     entry.created,
                     collection.name,
                     collection.make_key(entry.config),
                     executor_id
                    ])
            self.conn.commit()
            return c.rowcount
        if self.executor.submit(_helper).result() != 1:
            raise Exception("Setting value to unannouced config: {}/{}".format(entry.collection.name, entry.config))

    def get_entry_by_config(self, collection, config):
        key = collection.make_key(config)

        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT value, created FROM entries WHERE collection = ? AND key = ? AND (value is not null OR executor is null OR executor in (SELECT id FROM executors WHERE {}))".format(self.LIVE_EXECUTOR_QUERY),
                    [collection.name, key])
            return c.fetchone()
        result = self.executor.submit(_helper).result()
        if result is None:
            return None
        return Entry(config, pickle.loads(result[0]) if result[0] is not None else None, result[1])

    def has_entry_by_key(self, collection, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT COUNT(*) FROM entries WHERE collection = ? AND key = ? AND value is not null",
                      [collection.name, key])
            return bool(c.fetchone()[0])
        return self.executor.submit(_helper).result()

    def get_entry_state(self, collection, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT value is not null FROM entries WHERE collection = ? AND key = ? AND (value is not null OR executor is null OR executor in (SELECT id FROM executors WHERE {}))".format(self.LIVE_EXECUTOR_QUERY),
                      [collection.name, key])
            v = c.fetchone()
            if v is None:
                return None
            if v[0]:
                return "finished"
            else:
                return "announced"
        return self.executor.submit(_helper).result()

    """
    def get_entry_by_key(self, collection, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT config, value, created FROM entries WHERE collection = ? AND key = ?",
                    [collection.name, key])
            return c.fetchone()
        result = self.executor.submit(_helper).result()
        if result is None:
            return None
        config, value, created = result
        return Entry(collection, pickle.loads(config), pickle.loads(value), created)
    """

    def remove_entry_by_key(self, collection, key):
        def _helper():
            self.conn.execute("DELETE FROM entries WHERE collection = ? AND key = ?",
                [collection.name, key])
        self.executor.submit(_helper).result()

    def remove_entries(self, collection_key_pairs):
        def _helper():
            self.conn.executemany("DELETE FROM entries WHERE collection = ? AND key = ?", collection_key_pairs)
        self.executor.submit(_helper).result()

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
        return self.executor.submit(_helper).result()

    def _cleanup_lost_entries(self, cursor):
        cursor.execute("DELETE FROM entries WHERE value is null AND executor IN (SELECT id FROM executors WHERE {})".format(self.DEAD_EXECUTOR_QUERY))

    def announce_entries(self, executor_id, refs, deps):
        def _helper():
            c = self.conn.cursor()
            self._cleanup_lost_entries(c)
            self.conn.commit()
            try:
                c.executemany("INSERT INTO entries(collection, key, config, executor) VALUES (?, ?, ?, ?)",
                    [[r.collection.name,
                      r.collection.make_key(r.config),
                      pickle.dumps(r.config),
                      executor_id] for r in refs])
                """
                c.executemany("INSERT INTO deps VALUES (?, ?, ?, ?)", [
                    [r1.collection.name,
                     r1.collection.make_key(r1.config),
                     r2.collection.name,
                     r2.collection.make_key(r2.config)
                    ] for r1, r2 in deps
                ])"""
                self.conn.commit()
                return True
            except sqlite3.IntegrityError as e:
                self.conn.rollback()
                return False
        return self.executor.submit(_helper).result()

    def entry_summaries(self, collection):
        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT key, config, length(value), value_repr, created FROM entries WHERE collection = ?", [collection.name])
            return [
                {"key": key, "config": pickle.loads(config), "size": value_size + len(config) if value_size else len(config), "value_repr": value_repr, "created": created}
                for key, config, value_size, value_repr, created in r.fetchall()
            ]
        return self.executor.submit(_helper).result()

    def register_executor(self, executor):
        assert executor.id is None
        def _helper():
            c = self.conn.cursor()
            c.execute("INSERT INTO executors(created, heartbeat, heartbeat_interval, stats, type, version, resources) VALUES (?, DATETIME('now'), ?, ?, ?, ?, ?)",
                    [executor.created,
                     executor.heartbeat_interval,
                     json.dumps(executor.get_stats()),
                     executor.executor_type,
                     executor.version,
                     executor.resources])
            self.conn.commit()
            executor.id = c.lastrowid
        self.executor.submit(_helper).result()

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
        return self.executor.submit(_helper).result()

    def update_heartbeat(self, id):
        def _helper():
            c = self.conn.cursor()
            c.execute("""UPDATE executors SET heartbeat = DATETIME('now') WHERE id = ? AND stats is not null""", [id])
            self.conn.commit()
        self.executor.submit(_helper).result()

    def update_stats(self, id, stats):
        def _helper():
            c = self.conn.cursor()
            c.execute("""UPDATE executors SET stats = ?, heartbeat = DATETIME('now') WHERE id = ?""", [json.dumps(stats), id])
            self.conn.commit()
        self.executor.submit(_helper).result()

    def update_executor_stats(self, uuid, stats):
        assert stats != None
        raise NotImplementedError

    def stop_executor(self, id):
        def _helper():
            c = self.conn.cursor()
            c.execute("""UPDATE executors SET heartbeat = DATETIME('now'), stats = null WHERE id = ?""", [id])
            c.execute("""DELETE FROM entries WHERE executor == ? AND value is null""", [id])
            self.conn.commit()
        self.executor.submit(_helper).result()
