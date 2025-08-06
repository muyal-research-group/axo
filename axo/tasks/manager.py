from axo.models import Task,TaskStatus
from typing import Dict,Optional,List
import time as T

class TasksManager():
    def __init__(self):
        self.pending_tasks:Dict[str,Task]   = {}
        self.completed_tasks:Dict[str,Task] = {}
    def add_task(self, task: Task) -> None:
        if self.is_completed_by_task_id(task.id) or task.status != TaskStatus.PENDING:
            return None
        self.pending_tasks[task.id] = task
    def add_tasks(self,tasks:List[Task]):
        for t in tasks:
            self.add_task(t)
    
    def complete_task(self, task_id: str) -> bool:
        task = self.pending_tasks.pop(task_id, None)
        if task:
            self.completed_tasks[task_id] = task
            return True
        return False

    def get_task(self, task_id: str) -> Optional[Task]:
        return self.pending_tasks.get(task_id) or self.completed_tasks.get(task_id)
    
    def is_completed_by_task_id(self,task_id:str):
        return task_id in self.completed_tasks
    
    def remove_task(self, task_id: str) -> None:
        self.pending_tasks.pop(task_id, None)
        self.completed_tasks.pop(task_id, None)

    def __find_tasks_by_metadata(self,tasks:Dict[str,Task], key: str, value: str) -> list[Task]:
        result = []
        for task in tasks.values():
            if task.metadata.get(key) == value:
                result.append(task)
        return result
    def find_completed_tasks_by_metadata(self, key: str, value: str) -> list[Task]:
        return self.__find_tasks_by_metadata(tasks= self.completed_tasks, key=key,value=value)
    def find_pending_tasks_by_metadata(self, key: str, value: str) -> list[Task]:
        return self.__find_tasks_by_metadata(tasks= self.pending_tasks, key=key,value=value)