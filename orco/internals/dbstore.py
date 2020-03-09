import enum
import os
import pathlib
import pickle
import re
import uuid
from itertools import chain

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    Sequence,
    String,
    Table,
    Text,
    event,
    orm,
)
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
Session = orm.sessionmaker()

_SQF_KEY = "_sqlite_files_dir"
_SQF_DEL_KEY = "_sqlite_delete_list"
SQLITE_BLOB_DIR_SUFFIX = ".files"
SQLITE_BLOB_LIMIT = 4096


class Executor(Base):
    __tablename__ = "executors"

    id = Column(Integer, primary_key=True)
    heartbeat = Column(DateTime(timezone=True), nullable=False, server_default=sa.sql.func.now())
    heartbeat_interval = Column(Float, nullable=False)
    stats = Column(Text(), nullable=True)
    created = Column(DateTime(timezone=True), nullable=False, server_default=sa.sql.func.now())
    name = Column(String(), nullable=False)
    hostname = Column(String(), nullable=False)
    resources = Column(JSON, default={}, nullable=False)

    jobs = orm.relationship("Job", back_populates="executor")


class Builder(Base):
    __tablename__ = "builders"

    id = Column(Integer, primary_key=True)
    namespace = Column(String(), nullable=True, default="")
    name = Column(String(), nullable=False)
    pickled = Column(LargeBinary, nullable=True)

    entries = orm.relationship("Entry", back_populates="builder")
    _namespace_name_index = sa.Index((namespace, name), unique=True)


class Entry(Base):
    __tablename__ = "entries"

    id = Column(Integer, primary_key=True)
    builder_id = Column(ForeignKey(Builder.id))
    config_key = Column(String, nullable=False)
    config = Column(JSON, nullable=False)

    jobs = orm.relationship("Job", back_populates="entry")
    builder = orm.relationship("Builder", back_populates="entries")
    _builder_key_index = sa.Index((builder_id, config_key), unique=True)


job_deps_table = Table(
    "job_deps",
    Base.metadata,
    Column(
        "job_id", Integer, ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    ),
    Column(
        "dependency_id",
        Integer,
        ForeignKey("jobs.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class JobStatus(enum.Enum):
    READY = "ready"
    WAITING = "waiting"
    RUNNING = "runnnig"
    DONE = "done"
    FAILED = "failed"


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    entry_id = Column(sa.ForeignKey(Entry.id), nullable=False)
    created = Column(DateTime(timezone=True), nullable=False, server_default=sa.sql.func.now())
    executor_id = Column(ForeignKey(Executor.id), nullable=False)
    status = Column(Enum(JobStatus), nullable=False)

    executor = orm.relationship("Executor", back_populates="jobs")
    entry = orm.relationship("Entry", back_populates="jobs")
    result_files = orm.relationship("ResultFile", back_populates="job")
    dependencies = orm.relationship(
        "Job",
        secondary=job_deps_table,
        back_populates="dependants",
        primaryjoin=(id == job_deps_table.c.job_id),
        secondaryjoin=(id == job_deps_table.c.dependency_id),
    )
    dependants = orm.relationship(
        "Job",
        secondary=job_deps_table,
        back_populates="dependants",
        primaryjoin=(id == job_deps_table.c.dependency_id),
        secondaryjoin=(id == job_deps_table.c.job_id),
    )

    def clear_files(self, session):
        for rf in self.result_files:
            rf.clear_data()
            session.delete(rf)


class ResultFile(Base):
    __tablename__ = "result_files"

    # Restricted deletion to enforce manual deletion of this object, freeing any external files
    job_id = Column(ForeignKey(Job.id, ondelete="RESTRICT"), primary_key=True, nullable=False)
    # Null for the pickled main result
    name = Column(String, nullable=True, primary_key=True)
    # Created (or last overwritten)
    created = Column(DateTime(timezone=True), nullable=False, server_default=sa.sql.func.now())
    # Either one must be Null, both if file was removed
    _data_blob = Column("data", LargeBinary, nullable=True)
    _data_filename = Column("sqlite_filename", String, nullable=True)

    job = orm.relationship("Job", back_populates="result_files")

    def __init__(self, *args, data=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._data = data
        self._del_files = []
        self._data_dirty = True

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, val):
        self.clear_data()
        self._data = val

    def clear_data(self):
        if self._data_filename:
            self._del_files.append(self._data_filename)
        self._data_filename = None
        self._data_dirty = True
        self._data = None
        orm.attributes.flag_dirty(self)

    def _before_flush(self, session):
        "Prepare the object data for writing (if dirty)."
        sp = session.info[_SQF_KEY]
        if self._data_dirty:
            if sp and self._data and len(self._data) > SQLITE_BLOB_LIMIT:
                self._data_filename = str(uuid.uuid4())
                self._data_blob = None
                (sp / self._data_filename).write_bytes(self._data)
            else:
                self._data_filename = None
                self._data_blob = self._data
            self._data_dirty = False

    def _after_flush(self, session):
        "Move filename(s) to be deleted to the session."
        session.info[_SQF_DEL_KEY].extend(self._del_files)
        self._del_files = []

    def _loaded(self, context):
        "Fired on load and reload from DB, reads the file into memory."
        self._data = self._data_blob
        self._del_files = []
        self._data_dirty = False
        if self._data_filename:
            assert not self._data
            sp = context.session.info[_SQF_KEY]
            assert sp
            self._data = (sp / self._data_filename).read_bytes()


def sqlite_path(session):
    """
    Get the Path object for the SQLite db path, or `None`.

    Returns `None` for other databases and SQLite in-memory DB.
    """
    assert session.bind
    url = str(session.bind.engine.url)
    m = re.search("^sqlite[^/]*://[^/]*/(.*)$", url)
    if not m or m.groups()[0] in ("", ":memory:"):
        return None
    return pathlib.Path(m.groups()[0]).absolute()


@event.listens_for(Session, "after_transaction_create")
def _result_file_transaction_create(session, _transaction):
    "Initialize session.info with file directory and list of files to unlink."
    if _SQF_KEY not in session.info:
        sp = sqlite_path(session)
        if sp is None:
            session.info[_SQF_KEY] = None
        else:
            dp = sp.with_suffix(sp.suffix + SQLITE_BLOB_DIR_SUFFIX)
            dp.mkdir(exist_ok=True)
            session.info[_SQF_KEY] = dp
        session.info.setdefault(_SQF_DEL_KEY, [])
    return session.info[_SQF_KEY]


@event.listens_for(Session, "before_flush")
def _result_file_before_flush(session, _flush_context, _instances):
    "Moves any oversize data to files."
    for i in chain(session.new, session.dirty, session.deleted):
        if isinstance(i, ResultFile):
            i._before_flush(session)


@event.listens_for(Session, "after_flush")
def _result_file_before_flush(session, _flush_context):
    "Moves any oversize data to files."
    for i in chain(session.new, session.dirty, session.deleted):
        if isinstance(i, ResultFile):
            i._after_flush(session)


@event.listens_for(Session, "after_commit")
def _result_file_after_commit(session):
    "Remove any obsolte data files."
    sp = session.info[_SQF_KEY]
    for fname in session.info[_SQF_DEL_KEY]:
        fp = sp / fname
        try:
            fp.unlink()
        except FileNotFoundError:
            pass
    session.info[_SQF_DEL_KEY] = []


@event.listens_for(ResultFile, "load")
def _result_file_load(target, context):
    target._loaded(context)


@event.listens_for(ResultFile, "refresh")
def _result_file_refresh(target, context, _changed):
    # May needlessly reload the data, but DB updates without changing data should be rare
    target._loaded(context)


@event.listens_for(ResultFile, "before_delete")
def _result_file_before_delete(_mapper, _connection, target):
    target.clear_data()


def test():
    engine = sa.create_engine("sqlite:///testdb.sqlite", echo=True)
    Session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = Session()

    ex = Executor(name="bar", hostname="foo", resources={}, heartbeat_interval=6.0)
    bu = Builder(name="boo")
    en = Entry(builder=bu, config={}, config_key="{,}")
    jo = Job(executor=ex, entry=en, status=JobStatus.READY)
    rf = ResultFile(name="a", data=b"X" * 5000, job=jo)
    s.add(rf)
    s.commit()
    os.system("ls -la testdb.sqlite.files")

    rf2 = s.query(ResultFile).filter(ResultFile.name == "a").first()
    print("data len ", len(rf2.data))
    rf2.data = None
    s.add(rf2)
    s.commit()
    os.system("ls -la testdb.sqlite.files")

    rf3 = s.query(ResultFile).filter(ResultFile.name == "a").first()
    print("data", rf3.data)

    rf4 = ResultFile(name="b", data=b"X" * 6000, job=jo)
    s.add(rf4)
    s.commit()
    os.system("ls -la testdb.sqlite.files")

    rf5 = s.query(ResultFile).filter(ResultFile.name == "b").first()
    s.delete(rf5)
    s.commit()
    os.system("ls -la testdb.sqlite.files")

    print([r.name for r in jo.result_files])


if __name__ == "__main__":
    test()
