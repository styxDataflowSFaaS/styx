from enum import Enum, auto


class LocalStateBackend(Enum):
    DICT = auto()
    HYBRID_LOG = auto()
    LSM = auto()
    APPEND_LOG = auto()
