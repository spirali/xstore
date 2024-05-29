import enum

EntryId = int


class AnnounceResult(enum.Enum):
    FINISHED = 0
    COMPUTE_HERE = 1
    COMPUTING_ELSEWHERE = 2
