import pytest
import time as T
from axo.scheduler import AxoScheduler
from queue import Queue
import asyncio
import humanfriendly as HF
from axo.models import Task


@pytest.fixture
def scheduler():
    s = AxoScheduler(
        runtime_queue= Queue(maxsize=100),
        tasks=[]
    )
    return s


@pytest.mark.asyncio
async def test_uknown_operation(scheduler):
    """UKNOWN operation """
    scheduler.schedule(
        task=Task(operation="UKNOWN", executed_at=T.time() + 5)
    )
    N= 10 
    for i in range(N):
        print(scheduler.tm.pending_tasks)
        await asyncio.sleep(1)
    # while True
    # await asyncio.sleep(5)


@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_uknown_operation_execute_before_10s(scheduler):
    """UKNOWN operation """
    scheduler.schedule(task=Task(operation="UKNOWN", executed_at=T.time() + HF.parse_timespan("10s") ))
    await asyncio.sleep(15)

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_put_operation_execute_before_2s_not_found_path(scheduler):
    """UKNOWN operation """
    scheduler.schedule(task=Task(operation="PUT", executed_at=T.time() + HF.parse_timespan("2s"), max_waiting_time="5s" ))
    await asyncio.sleep(15)
@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_stop(scheduler):
    """UKNOWN operation """
    scheduler.stop()
    # scheduler.schedule(task=Task(operation="PUT", executed_at=T.time() + HF.parse_timespan("2s"), max_waiting_time="5s" ))
    await asyncio.sleep(5)