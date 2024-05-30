from typing import Any, Iterable

import sqlalchemy as sa
from datetime import datetime

from .ref import Ref
from .entry import AnnounceResult, EntryId, Entry


def _set_sqlite_pragma(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class Database:
    def __init__(self, url):
        engine = sa.create_engine(url)
        if "sqlite" in engine.dialect.name:
            sa.event.listen(engine, "connect", _set_sqlite_pragma)
        self.url = url
        metadata = sa.MetaData()
        self.entries = sa.Table(
            "entries",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String(80)),
            sa.Column("version", sa.Integer),
            sa.Column("config_key", sa.String(56)),  # 56 = hexdigest of sha224
            sa.Column("replica", sa.Integer),
            sa.Column("config", sa.PickleType),
            sa.Column("result", sa.PickleType),
            sa.Column(
                "created_date",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.sql.func.now(),
            ),
            sa.Column(
                "finished_date",
                sa.DateTime(timezone=True),
                nullable=True,
            ),
            sa.Column("run_info", sa.JSON),
            sa.UniqueConstraint("name", "version", "config_key", "replica"),
            # sa.Column("computation_time", sa.Integer(), nullable=True),
            # sa.Index("key_idx", "key"),
        )

        self.deps = sa.Table(
            "deps",
            metadata,
            sa.Column(
                "source_id",
                sa.Integer(),
                sa.ForeignKey("entries.id", ondelete="cascade"),
            ),
            sa.Column(
                "target_id",
                sa.Integer(),
                sa.ForeignKey("entries.id", ondelete="cascade"),
            ),
            sa.UniqueConstraint("source_id", "target_id"),
        )

        # self.blobs = sa.Table(
        #     "blobs",
        #     metadata,
        #     sa.Column("entry_id", sa.ForeignKey("entries.id", ondelete="cascade"), primary_key=True),
        #     sa.Column("name", sa.String, nullable=False, primary_key=True),
        #     sa.Column("data", sa.LargeBinary, nullable=False),
        #     sa.Column("mime", sa.String(255), nullable=False),
        #     sa.Column("repr", sa.String(85), nullable=True),
        # )

        self.metadata = metadata
        self.engine = engine

    def read_entry(self, ref) -> Entry:
        c = self.entries.c
        with self.engine.connect() as conn:
            select = (
                sa.select(c.id, c.result, c.finished_date)
            )
            if ref.entry_id is None:
                select = (select.where(c.name == ref.name)
                               .where(c.version == ref.version)
                               .where(c.config_key == ref.config_key)
                               .where(c.replica == ref.replica)
                )
            else:
                select = select.where(c.id == ref.entry_id)
            r = conn.execute(select).one_or_none()
            if r is not None:
                return Entry(entry_id=r[0], ref=ref, result=r[1], finished_date=r[2])
            else:
                return None

    def read_refs(self, name):
        c = self.entries.c
        with self.engine.connect() as conn:
            select = sa.select(
                c.id, c.version, c.config, c.config_key, c.replica
            ).where(c.name == name)
            return [
                Ref(
                    name,
                    version,
                    config,
                    replica,
                    entry_id=entry_id,
                    config_key=config_key,
                )
                for entry_id, version, config, config_key, replica in conn.execute(
                    select
                ).all()
            ]

    def read_result(self, ref) -> Any:
        c = self.entries.c
        with self.engine.connect() as conn:
            select = (
                sa.select(c.result)
                .where(c.name == ref.name)
                .where(c.version == ref.version)
                .where(c.config_key == ref.config_key)
                .where(c.replica == ref.replica)
            )
            r = conn.execute(select).one_or_none()
            if r is not None:
                return r[0]
            else:
                return None

    def get_or_announce_entry(self, ref) -> (AnnounceResult, EntryId, Any):
        c = self.entries.c
        with self.engine.connect() as conn:
            try:
                stmt = (
                    sa.insert(self.entries)
                    .values(
                        name=ref.name,
                        version=ref.version,
                        config=ref.config,
                        config_key=ref.config_key,
                        replica=ref.replica,
                    )
                    .returning(self.entries.c.id)
                )
                r = conn.execute(stmt).one_or_none()
                conn.commit()
                return AnnounceResult.COMPUTE_HERE, r[0], None
            except sa.exc.IntegrityError:
                select = (
                    sa.select(c.id, c.result, c.finished_date)
                    .where(c.name == ref.name)
                    .where(c.version == ref.version)
                    .where(c.config_key == ref.config_key)
                    .where(c.replica == ref.replica)
                )
                r = conn.execute(select).one()
                if r[2] is None:
                    return AnnounceResult.COMPUTING_ELSEWHERE, r[0], None
                else:
                    return AnnounceResult.FINISHED, r[0], r[1]

    def finish_entry(self, entry_id, result, run_info, deps: Iterable[EntryId]):
        with self.engine.connect() as conn:
            stmt = (
                sa.update(self.entries)
                .where(self.entries.c.id == entry_id)
                .values(result=result, run_info=run_info, finished_date=datetime.now())
            )
            conn.execute(stmt)
            if deps:
                conn.execute(
                    sa.insert(self.deps),
                    [{"source_id": dep, "target_id": entry_id} for dep in deps],
                )

            conn.commit()

    def cancel_entry(self, entry_id):
        with self.engine.connect() as conn:
            stmt = sa.delete(self.entries).where(self.entries.c.id == entry_id)
            conn.execute(stmt)
            conn.commit()

    def init(self):
        self.metadata.create_all(self.engine)
