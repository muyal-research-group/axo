from __future__ import annotations

# ────────────────────────────────────────────────────────────────── stdlib ──
import logging
import string
import time
from abc import ABC, abstractmethod
from queue import Queue
from typing import Optional,TYPE_CHECKING
from weakref import WeakKeyDictionary

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
from nanoid import generate as nanoid
from option import Err, Result,Ok

# ──────────────────────────────────────────────────────────────── project ───
from axo.scheduler import Scheduler, Task
from axo.storage.services import StorageService

from axo.types import EndpointManagerP
from axo.log import get_logger
if TYPE_CHECKING:
    from axo.core.axo import Axo

#     LocalEndpointManager,
#     DistributedEndpointManager,EndpointX
# )  # only for typing / re‑export convenience

# ────────────────────────────────────────────────────────────── logging set‑up
logger = get_logger(name=__name__)

# ─────────────────────────────────────────────────────────────── typing alias



# recombine the concrete types for external code completion
# EndpointManager = LocalEndpointManager | DistributedEndpointManager


# =========================================================================== #
# Runtime
# =========================================================================== #
class ActiveXRuntime(ABC):
    """
    Base class for Axo runtimes.

    Works with *any* endpoint manager that satisfies :class:`EndpointManagerP`.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #

    @property
    @abstractmethod
    def runtime_id(self)->str:
        ...

    @property
    @abstractmethod
    def is_distributed(self)->bool:
        ...
    @property
    @abstractmethod
    def q(self)->Queue[Task]:
        ...
    
    @property
    @abstractmethod
    def scheduler(self)->Scheduler:
        ...
    @property
    @abstractmethod
    def storage_service(self)->StorageService:
        ...
    @property
    @abstractmethod
    def endpoint_manager(self)->EndpointManagerP:
        ...
    @property
    @abstractmethod
    def inmemory_objects(self)->WeakKeyDictionary[Axo,None]:
        ...
    @property
    @abstractmethod
    def is_running(self)->bool:
        ...
    # ------------------------------------------------------------------ #
    # Persist & fetch helpers
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def get_active_object(self, *, bucket_id: str, key: str) -> Result[Axo, Exception]:
        ...
    @abstractmethod
    async def persistify(
        self, instance: Axo, *, bucket_id: str = "axo", key: Optional[str] = None
    ) -> Result[str, Exception]:
        ...

    # ------------------------------------------------------------------ #
    # Thread lifecycle
    # ------------------------------------------------------------------ #
    @abstractmethod
    def stop(self) -> None: ...

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    @abstractmethod
    def _handle_put_task(self, task: Task) -> None:
        ...
    