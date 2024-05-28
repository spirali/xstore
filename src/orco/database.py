from typing import Any

import sqlalchemy as sa

from .ref import Ref


def _set_sqlite_pragma(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class Database:
    def __init__(self, url):
        engine = sa.create_engine(url)
        self.url = url

        metadata = sa.MetaData()
        self.entries = sa.Table(
            "entries",
            metadata,
            sa.Column("name", sa.String(80), primary_key=True),
            sa.Column("version", sa.Integer, primary_key=True),
            sa.Column(
                "config_key", sa.String(56), primary_key=True
            ),  # 56 = hexdigest of sha224
            sa.Column("replica", sa.Integer, primary_key=True),
            sa.Column("config", sa.PickleType),
            sa.Column("result", sa.PickleType),
            sa.Column(
                "created_date",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.sql.func.now(),
            ),
            sa.Column("run_info", sa.JSON),
            # sa.Column("computation_time", sa.Integer(), nullable=True),
            # sa.Index("key_idx", "key"),
        )
        self.metadata = metadata
        self.engine = engine
        self.conn = engine.connect()

    def get_results(self, refs: list[Ref]) -> dict[Ref, Any]:
        c = self.entries.c
        # TODO: Do it as one query

        # select = sa.select([c.result]).where(
        #    sa.tuple_(c.name, c.config_key, c.version, c.replica).in_([r.tuple_key for r in refs]))
        # self.conn.execute(select)

        result = {}
        for ref in refs:
            select = (
                sa.select(c.result)
                .where(c.name == ref.name)
                .where(c.version == ref.version)
                .where(c.config_key == ref.config_key)
                .where(c.replica == ref.replica)
            )
            r = self.conn.execute(select).one_or_none()
            if r is not None:
                result[ref] = r[0]
        return result

    def insert(self, ref, result, run_info):
        print("INSERT", result)
        stmt = sa.insert(self.entries).values(
            name=ref.name,
            version=ref.version,
            config=ref.config,
            config_key=ref.config_key,
            replica=ref.replica,
            result=result,
            run_info=run_info,
        )
        self.conn.execute(stmt)

    def init(self):
        self.metadata.create_all(self.engine)
