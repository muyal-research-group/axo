from __future__ import annotations

# ────────────────────────────────────────────────────────────────── stdlib ──
from abc import ABC, abstractmethod
import json as J
from queue import Queue
from typing import Optional,TYPE_CHECKING
from weakref import WeakKeyDictionary
# ─────────────────────────────────────────────────────────────── 3rd‑party ──
from option import Result,Err,Ok

# ──────────────────────────────────────────────────────────────── project ───
from axo.scheduler import Scheduler, Task
from axo.storage import AxoStorage
from axo.storage.loader import AxoLoader
from axo.errors import AxoError,AxoErrorType
from axo.types import EndpointManagerP
from axo.log import get_logger
from axo.storage.types import AxoObjectBlob, AxoObjectBlobs, AxoStorageMetadata
from axo.storage.utils import StorageUtils as SU
from axo.types import EndpointManagerP  # your protocol

if TYPE_CHECKING:
    from axo.core.axo import Axo


# ────────────────────────────────────────────────────────────── logging set‑up
logger = get_logger(name=__name__)

# ─────────────────────────────────────────────────────────────── typing alias


# =========================================================================== #
# Runtime
# =========================================================================== #
class ActiveXRuntime(ABC):
    """
    Base class for Axo runtimes, storage-agnostic via AxoStorage/AxoLoader.
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
    def axo_storage(self) -> AxoStorage: ...

    @property
    @abstractmethod
    def axo_loader(self) -> AxoLoader: ...
    


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
    async def get_active_object(self, *, bucket_id: str, key: str) -> Result[Axo, AxoError]:
        ...

    @abstractmethod
    async def persistify(
        self, instance: Axo, *, bucket_id: str = "axo", key: Optional[str] = None
    ) -> Result[str, AxoError]:
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
