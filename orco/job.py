import collections

from orco.consts import MIME_PICKLE, MIME_TEXT
import pickle
import tarfile
import io
import os
import enum


class JobState(enum.Enum):
    NONE = "n"
    ANNOUNCED = "a"
    RUNNING = "r"
    FINISHED = "f"
    ERROR = "e"
    ARCHIVED = "v"
    FREED = "d"


ACTIVE_STATES = (JobState.ANNOUNCED, JobState.RUNNING, JobState.FINISHED, JobState.FREED)

JobMetadata = collections.namedtuple("EntryMetadata", ["created_date", "computation_time", "finished_date", "job_setup"])


class _NoValue:
    pass


_NO_VALUE = _NoValue()


class Job:
    """
    A single computation with its configuration and result

    Attributes
    * config - `OrderedDict` of function parameters (use with `Builder.run_with_config`)
    * value - resulting value of the computation
    * job_setup - setup for the job
    * created - datetime when job was created
    * comp_time - time of computation when job was created, or None if job was inserted
    """

    __slots__ = ("builder_name", "key", "config", "state", "_value", "_job_id", "_db")

    def __init__(self, builder_name, key, config):
        self.builder_name = builder_name
        self.key = key
        self.config = config

        self.state = None
        self._job_id = None
        self._db = None
        self._value = _NO_VALUE

    @property
    def value(self):
        self._check_attached()
        if self.state != JobState.FINISHED:
            raise Exception("Job is not finished")
        value = self._value
        if value is not _NO_VALUE:
            return value
        value, mime = self._db.get_blob(self._job_id, None)
        if value is None:
            self._value = None
            return None
        value = pickle.loads(value)
        self._value = value
        return value

    def is_attached(self):
        return self._job_id is not None

    def detach(self):
        self._job_id = None
        self._db = None
        self.state = None

    def set_job_id(self, job_id, db, state):
        assert self._job_id is None
        self.state = state
        self._job_id = job_id
        self._db = db

    def metadata(self):
        self._check_attached()
        return self._db.read_metadata(self._job_id)

    def get_object(self, name, default=_NO_VALUE):
        value, mime = self.get_blob(name, default)
        if mime != MIME_PICKLE:
            raise Exception("Blob exists, but is not pickled object, but {}".format(mime))
        return pickle.loads(value)

    def get_text(self, name):
        value, mime = self.get_blob(name)
        if mime != MIME_TEXT:
            raise Exception("Blob exists, but is not text, but {}".format(mime))
        return value.decode()

    def get_names(self):
        self._check_attached()
        return self._db.get_blob_names(self._job_id)

    def get_blob(self, name, default=_NO_VALUE):
        self._check_attached()
        value, mime = self._db.get_blob(self._job_id, name)
        if value is None:
            if default is _NO_VALUE:
                raise Exception("Blob '{}' not found".format(name))
            return default
        return value, mime

    def get_blob_as_file(self, name, target=None):
        value, _ = self.get_blob(name)
        if target is None:
            target = name
        with open(target, "wb") as f:
            f.write(value)

    def extract_tar(self, name, target=None):
        value, mime = self.get_blob(name)
        if mime != "application/tar":
            raise Exception("Blob is not tar archive")
        if target is None:
            target = name
        if not os.path.isdir(target):
            os.makedirs(target)
        with tarfile.TarFile(fileobj=io.BytesIO(value)) as tf:
            tf.extractall(target)

    def _check_attached(self):
        if self._job_id is None:
            raise Exception("Job is not attached")

    def __repr__(self):
        return "<Entry {}/{}>".format(self.builder_name, self.key)