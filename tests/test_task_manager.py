import pytest
from axo.tasks.manager import TasksManager
from axo.models import Task,TaskStatus

@pytest.fixture
def task_manager():
    tm = TasksManager()
    tm.add_tasks([
        Task(
            operation="X",
            executed_at=-1,
            max_waiting_time="1m",
            metadata={"key1": "valuE"}
        ),
        Task(
            operation="X",
            executed_at=-1,
            max_waiting_time="1m",
            metadata={"key1": "value"}
        )
    ])
    return tm

def test_tm_add_task(task_manager):
    task_manager.add_task(Task(operation="X"))
    assert len(task_manager.pending_tasks)==3

def test_tm_add_tasks(task_manager):
    N = 10 
    task_manager.add_tasks([Task(operation="X") for i in range(N)])
    print("TM_SIZE", len(task_manager.pending_tasks))
    assert len(task_manager.pending_tasks) == N+2

def test_tm(task_manager):
    res = task_manager.find_pending_tasks_by_metadata(key="key1", value="valuE")
    print(res)
    assert len(res)>=1

def test_tm_remove(task_manager):
    res = task_manager.find_pending_tasks_by_metadata(key="key1", value="valuE")
    task_manager.remove_task(res[0].id)
    assert len(task_manager.pending_tasks) ==1




