import enum
from dataclasses import dataclass
from typing import Any
from .ref import Ref


EntryId = int


class AnnounceResult(enum.Enum):
    FINISHED = 0
    COMPUTE_HERE = 1
    COMPUTING_ELSEWHERE = 2


@dataclass
class Entry:
    entry_id: int
    ref: Ref
    result: Any
