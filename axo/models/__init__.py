from nanoid import generate as nanoid
import string
import humanfriendly as HF
from typing import Dict
import time as T
from enum import Enum,auto
ALPHABET = string.digits+string.ascii_lowercase

class TaskStatus(Enum):
    # CREATED = auto()
    PENDING   = auto()
    RUNNING   = auto()
    SUCCESS   = auto()
    FAILED    = auto()
    TIMEOUT   = auto()
    CANCELLED = auto()

class Task:
    def __init__(self,
        operation:str,
        executed_at:float = -1,
        max_waiting_time:str = "1m",
        metadata:Dict[str,str]={}
    ) -> None:
        self.id = nanoid(alphabet=ALPHABET)
        self.created_at = T.time()
        if executed_at < self.created_at :
            self.executes_at = self.created_at
        else:
            self.executes_at  = self.created_at if executed_at < 0 else executed_at
        self.waiting_time = 0
        self.operation= operation
        self.metadata = metadata
        self.max_waiting_time = HF.parse_timespan(max_waiting_time)
        self.status = TaskStatus.PENDING
    def get_formatted_max_waiting_time(self):
        return HF.format_timespan(self.max_waiting_time)
    def get_formatted_waiting_time(self):
        return HF.format_timespan(self.waiting_time)
    def __str__(self) -> str:
        return "Task(id={}, operation={})".format(self.id,self.operation)
    