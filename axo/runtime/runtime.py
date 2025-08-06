"""
axo/runtime/core.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Thread‑based runtime backbone for Axo.

This refactor removes the hard dependency on *EndpointManagerX* and replaces
it with a lightweight *protocol* (`EndpointManagerP`) so the same runtime can
be used with:

* :class:`axo.endpoint.LocalEndpointManager`
* :class:`axo.endpoint.DistributedEndpointManager`
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────── stdlib ──
import logging
import string
import time
from abc import ABC, abstractmethod
from queue import Queue
from typing import Optional
import time as T

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
import cloudpickle as cp
from nanoid import generate as nanoid
from option import Err, Result,Ok

# ──────────────────────────────────────────────────────────────── project ───
from axo.scheduler import Scheduler, Task
from axo.storage.data import Axo, StorageService
from axo.types import EndpointManagerP
from axo.endpoint.endpoint import EndpointX
from axo.log import get_logger

from weakref import WeakKeyDictionary
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




    # def __init__(
    #     self,
    #     *,
    #     q: Queue,
    #     endpoint_manager: EndpointManagerP[EndpointX],
    #     storage_service: StorageService,
    #     scheduler: Scheduler,
    #     runtime_id: str = "",
    #     is_distributed: bool = False,
    # ) -> None:
    #     super().__init__(name=runtime_id, daemon=True)

    #     # identifiers & flags -------------------------------------------
    #     self.runtime_id = (
    #         runtime_id if len(runtime_id) >= 16 else nanoid(alphabet=_ALPHABET)
    #     )
    #     self.is_distributed = is_distributed

    #     # collaborators --------------------------------------------------
    #     self.q: Queue[Task] = q
    #     self.scheduler = scheduler
    #     self.storage_service = storage_service
    #     self.endpoint_manager: EndpointManagerP = endpoint_manager

    #     # caches ---------------------------------------------------------
    #     self.inmemory_objects: WeakKeyDictionary[Axo, None] = WeakKeyDictionary()
    #     self.remote_files: set[str] = set()

    #     # misc helpers ---------------------------------------------------
    #     self.import_manager = DefaultImportManager()

    #     # thread ctl -----------------------------------------------------
    #     self._running = True
    #     self.start()

    # ------------------------------------------------------------------ #
    # Persist & fetch helpers
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def get_active_object(self, *, bucket_id: str, key: str) -> Result[Axo, Exception]:
        pass
        # return await self.storage_service._get_active_object(bucket_id=bucket_id, key=key)

    @abstractmethod
    async def persistify(
        self, instance: Axo, *, bucket_id: str = "axo", key: Optional[str] = None
    ) -> Result[str, Exception]:
        pass
        # """
        # Serialize an active object and store (1) its pickled class‐def, and
        # (2) its state bytes.  Uses whatever endpoint manager was injected.
        # """
        # try:
        #     t1 = T.time()
        #     key      = key or instance.get_axo_key()
        #     endpoint:EndpointX = self.endpoint_manager.get_endpoint(instance.get_endpoint_id())
        #     meta_res:Result[str, Exception] = endpoint.put(key=key, metadata=instance._acx_metadata)
        #     if meta_res.is_err:
        #         logger.error({
        #             "error":str(meta_res.unwrap_err())
        #         })
        #         return Err(meta_res.unwrap_err())
        #     logger.info({
        #         "event":"ENDPOINT.PUT",
        #         "response_time":T.time() - t1
        #     })
        #     # 2) class definition
        #     attrs,methods, class_def, class_code = instance.get_raw_parts()

        #     attrs_put_result = await self.storage_service.put(
        #         bucket_id=bucket_id,
        #         key = f"{key}_attrs",
        #         tags = {
        #             "module": instance._acx_metadata.axo_module,
        #             "class_name": instance._acx_metadata.axo_class_name,
        #         }, 
        #         data =cp.dumps(attrs)
        #     )
        #     if attrs_put_result.is_err:
        #         return Err(attrs_put_result.unwrap_err())

        #     # class_put_result = await self.storage_service.put(
        #     #     bucket_id=bucket_id,
        #     #     key=f"{key}_class_def",
        #     #     tags={
        #     #         "module": instance._acx_metadata.module,
        #     #         "class_name": instance._acx_metadata.class_name,
        #     #     },
        #     #     data=cp.dumps(class_def)
        #     # )
        #     # if class_put_result.is_err:
        #     #     return Err(class_put_result.unwrap_err())
        #     # methods_put_result = await self.storage_service.put(
        #     #     bucket_id=bucket_id,
        #     #     key = f"{key}_methods",
        #     #     tags = {
        #     #         "module": instance._acx_metadata.module,
        #     #         "class_name": instance._acx_metadata.class_name,
        #     #     }, 
        #     #     data = cp.dumps(methods)
        #     # )
        #     # if methods_put_result.is_err:
        #     #     return Err(methods_put_result.unwrap_err())
        #     tags = instance._acx_metadata.to_json_with_string_values()
        #     class_code_put_result = await self.storage_service.put(
        #         bucket_id=bucket_id,
        #         key = f"{key}_source_code",
        #         tags = {
        #             **tags
        #             # "module": instance._acx_metadata.module,
        #             # "class_name": instance._acx_metadata.class_name,
        #         }, 
        #         data = cp.dumps(class_code.encode("utf-8"))
        #     )
        #     if class_code_put_result.is_err:
        #         return Err(class_code_put_result.unwrap_err())



        #     # class_def_key = class_put_result.unwrap()

        #     # 3) object bytes
        #     # tags["class_def_key"] = class_def_key
        #     return Ok(key)
        #     # object_put_result = await self.storage_service.put(
        #     #     bucket_id=bucket_id, key=key, data=instance.to_bytes(), tags=tags
        #     # )
        #     # print("CLASS_DEF_KEY_RESULT",object_put_result)
            
        #     # return object_put_result
        # except Exception as exc:  # pragma: no cover
        #     logger.exception("persistify failed")
        #     return Err(exc)

    # ------------------------------------------------------------------ #
    # Thread lifecycle
    # ------------------------------------------------------------------ #
    @abstractmethod
    def stop(self) -> None: ...

    
    # def run(self) -> None:
        # """Main consumer loop."""
        # while self._running:
        #     task: Task = self.q.get()

        #     if time.time() < task.executes_at:
        #         self.q.put(task)
        #         continue

        #     if task.operation == "PUT":
        #         self._handle_put_task(task)
        #     elif task.operation == "DROP":
        #         logger.debug("DROP not implemented (%s)", task.id)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    @abstractmethod
    def _handle_put_task(self, task: Task) -> None:
        """Upload file referenced by *task* if not already stored."""
        pass
        # path = task.metadata.get("path", "")
        # if not path or path in self.remote_files:
        #     return

        # res = self.storage_service.put_data_from_file(
        #     bucket_id=task.metadata.get("bucket_id", "axo"),
        #     key="",
        #     source_path=path,
        #     tags={},
        #     chunk_size=task.metadata.get("chunk_size", "1MB"),
        # )
        # if res.is_ok:
        #     self.remote_files.add(path)
        #     logger.info("PUT ok %s", path)
        # else:
        #     logger.error("PUT failed %s → %s", path, res.unwrap_err())
