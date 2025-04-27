"""
axo/scheduler/scheduler.py
~~~~~~~~~~~~~~~~~~~~~

Thread‑based schedulers that decouple **task orchestration** from
**task execution** inside Axo.

Two classes are provided:

* :class:`Scheduler` – abstract, generic queue‑driven scheduler.
* :class:`ActiveXScheduler` – concrete subclass used by the default runtime.

The scheduler pulls :class:`axo.models.Task` objects from its *internal*
queue, decides whether they are ready to run, and—if so—pushes them onto
the *runtime* queue consumed by an :class:`axo.runtime.ActiveXRuntime`
instance.  This indirection lets us implement different scheduling
policies (priority, deadlines, etc.) without touching the runtime.
"""

from __future__ import annotations

import logging
import os
import time as T
from abc import ABC
from queue import Queue
from threading import Thread
from typing import List

from nanoid import generate as nanoid  # noqa: F401 (imported for future use)

import humanfriendly as HF

import axo.utils as utilx
from axo.models import Task

# --------------------------------------------------------------------------- #
# Logger
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(_h)
logger.setLevel(logging.DEBUG)


# ============================================================================
# Generic scheduler
# ============================================================================
class Scheduler(ABC, Thread):
    """
    Base class for queue‑driven schedulers.

    Parameters
    ----------
    runtime_q :
        Queue into which *ready* tasks are pushed for execution by the runtime.
    scheduler_name :
        Thread name (useful for debugging).
    tasks :
        Optional list of tasks to preload into the scheduler queue.
    maxsize :
        Maximum size of the internal queue (back‑pressure).
    """

    def __init__(
        self,
        *,
        runtime_q: Queue,
        scheduler_name: str = "axo-scheduler",
        tasks: List[Task] | None = None,
        maxsize: int = 100,
    ) -> None:
        super().__init__(name=scheduler_name, daemon=True)
        self.t1 = T.time()
        self.runtime_queue: Queue = runtime_q
        self.q: Queue[Task] = Queue(maxsize=maxsize)

        # Preload tasks (if any) ----------------------------------------
        for t in tasks or []:
            self.q.put(t)

        self._running = True
        self._heartbeat = 1.0  # seconds between retries
        self.start()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def schedule(self, task: Task) -> None:
        """Add a new task to the scheduler queue."""
        self.q.put(task)

    def stop(self) -> None:
        """Signal the scheduler thread to exit."""
        self._running = False
        logger.debug(f"SCHEDULER.STOP total_time={HF.format_timespan( T.time()-self.t1)}")

    # ------------------------------------------------------------------ #
    # Thread main loop
    # ------------------------------------------------------------------ #
    def run(self) -> None:  # noqa: C901 (complexity OK for this size)
        while self._running:
            task: Task = self.q.get()

            now = T.time()
            logger.debug(
                "SCHEDULER.DEQUEUE op=%s id=%s wait=%.2fs",
                task.operation,
                task.id,
                task.waiting_time,
            )

            # ---------------------------------------------------------- #
            # 1) Not yet time to run → re‑queue
            # ---------------------------------------------------------- #
            if task.executes_at > now:
                self._requeue(task, now)
                continue

            # ---------------------------------------------------------- #
            # 2) Task is due → handle by operation type
            # ---------------------------------------------------------- #
            if task.operation == "PUT":
                self._handle_put(task, now)
            else:
                # Unknown op → drop
                logger.warning("Unknown operation %s (dropping)", task.operation)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _requeue(self, task: Task, now: float) -> None:
        """Re‑queue task or drop it if it waited too long."""
        T.sleep(self._heartbeat)
        task.waiting_time = now - task.created_at

        if task.waiting_time >= task.max_waiting_time:
            logger.debug(f"SCHEDULER.DROP id={task.id} wt={task.get_formatted_waiting_time()} max_wt={task.get_formatted_max_waiting_time()}")
            self.runtime_queue.put(Task(operation="DROP", metadata={"task_id": task.id}))
        else:
            logger.debug(f"SCHEDULER.ENQUEUE id={task.id}")
            self.q.put(task)

    def _handle_put(self, task: Task, now: float) -> None:
        """Handle a PUT task (upload a file)."""
        path = task.metadata.get("path")
        if path and os.path.exists(path) and not utilx.is_writing(path):
            logger.debug("SCHEDULER.FORWARD.PUT id=%s path=%s", task.id, path)
            self.runtime_queue.put(
                Task(
                    operation="PUT",
                    metadata={"task_id": task.id, "path": path},
                )
            )
        else:
            logger.debug(f"FILE.NOT.READY id={task.id} path={path}")
            self._requeue(task=task, now=now)
            # # File not ready → retry later
            # time.sleep(self._heartbeat)
            # task.waiting_time = now - task.created_at
            # self.q.put(task)


# ============================================================================
# Default scheduler used by ActiveXRuntime
# ============================================================================
class AxoScheduler(Scheduler):
    """Concrete scheduler with default parameters suitable for most runtimes."""

    def __init__(self, *, runtime_queue: Queue, tasks: List[Task] | None = None) -> None:
        super().__init__(
            runtime_q=runtime_queue,
            scheduler_name="axo-scheduler",
            tasks=tasks,
            maxsize=100,
        )
