import os
from abc import ABC
from typing import Dict,List
from nanoid import generate as nanoid
import string
import logging
from queue import Queue
from threading import Thread
import time as T
import humanfriendly as HF
import activex.utils as UtilX

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

ALPHABET = string.digits+string.ascii_lowercase

class Task:
    def __init__(self,operation:str,executed_at:float = -1,metadata:Dict[str,str]={}) -> None:
        self.id = nanoid(alphabet=ALPHABET)
        self.created_at = T.time()
        if executed_at < self.created_at :
            self.executes_at = self.created_at
        else:
            self.executes_at  = self.created_at if executed_at < 0 else executed_at
        self.wainting_time = 0
        self.operation= operation
        self.metadata = metadata
        self.max_waiting_time = HF.parse_timespan("1m")
    def __str__(self) -> str:
        return "Task(id={}, operation={})".format(self.id,self.operation)
    
class Scheduler(ABC,Thread):
    def __init__(self,
                 runtime_q:Queue,
                 scheduler_name:str="activex-scheduler",
                 tasks:List[Task]=[],
                 maxsize:int=100
    ) -> None:
        Thread.__init__(self,daemon=True)
        self.setName(scheduler_name)
        self.runtime_queue = runtime_q
        self.q = Queue(maxsize=maxsize)
        self.tasks=tasks
        self.is_running = True
        self.heartbeat = 1
        self.start()

    def schedule(self,task:Task):
        self.q.put(task)
    def run(self) -> None:
        while self.is_running:
            task:Task = self.q.get()
            current_time = T.time()
            logger.debug("DEQUEUE {} {} {}".format(task.operation,task.id,task.wainting_time))

            if task.executes_at <= current_time:
                if task.operation == "PUT":
                    path = task.metadata.get("path",None)
                    if os.path.exists(path=path) and not UtilX.is_writing(path):
                        logger.debug("{} {} {}".format (task.operation,task.id,path) )
                        self.runtime_queue.put(
                            Task(
                                operation="PUT", 
                                metadata={
                                    "task_id": task.id,
                                    "path":path
                                }
                            )
                        )

                    else:
                        T.sleep(self.heartbeat)
                        task.wainting_time = current_time - task.created_at
                        logger.debug("SCHEDULER.ENQUEUE {}".format(task.id))
                        self.q.put(task)

            else:
                T.sleep(self.heartbeat)
                task.wainting_time = current_time - task.created_at
                if task.wainting_time >= task.max_waiting_time:
                    logger.debug("SCHEDULER.DROP {}".format(task.id))
                    self.runtime_queue.put(Task(operation="DROP", metadata={"task_id": task.id}))
                else:
                    self.q.put(task)
            

            
            # logger.debug("QUEUE.GET {}".format(item))
            # print("ITEM",item)

class ActiveXScheduler(Scheduler):
    def __init__(self, runtime_queue:Queue,tasks: List[Task] = []) -> None:
        Scheduler.__init__(self,
                           runtime_q=runtime_queue,
                           scheduler_name="activex-scheduler",
                           tasks=tasks,
                           maxsize=100
                           )