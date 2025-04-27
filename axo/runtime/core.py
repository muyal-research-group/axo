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
from threading import Thread
from typing import Optional
from weakref import WeakKeyDictionary

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
import cloudpickle as cp
from nanoid import generate as nanoid
from option import Err, Result

# ──────────────────────────────────────────────────────────────── project ───
from axo.import_manager import DefaultImportManager
from axo.scheduler import Scheduler, Task
from axo.storage.data import Axo, StorageService
from axo.types import EndpointManagerP
from axo.endpoint.endpoint import EndpointX
#     LocalEndpointManager,
#     DistributedEndpointManager,EndpointX
# )  # only for typing / re‑export convenience

# ────────────────────────────────────────────────────────────── logging set‑up
logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(_h)
logger.setLevel(logging.DEBUG)

# ─────────────────────────────────────────────────────────────── typing alias
_ALPHABET = string.digits + string.ascii_lowercase



# recombine the concrete types for external code completion
# EndpointManager = LocalEndpointManager | DistributedEndpointManager


# =========================================================================== #
# Runtime
# =========================================================================== #
class ActiveXRuntime(ABC, Thread):
    """
    Base class for Axo runtimes.

    Works with *any* endpoint manager that satisfies :class:`EndpointManagerP`.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        *,
        q: Queue,
        endpoint_manager: EndpointManagerP[EndpointX],
        storage_service: StorageService,
        scheduler: Scheduler,
        runtime_id: str = "",
        is_distributed: bool = False,
    ) -> None:
        super().__init__(name="axo-runtime", daemon=True)

        # identifiers & flags -------------------------------------------
        self.runtime_id = (
            runtime_id if len(runtime_id) >= 16 else nanoid(alphabet=_ALPHABET)
        )
        self.is_distributed = is_distributed

        # collaborators --------------------------------------------------
        self.q: Queue[Task] = q
        self.scheduler = scheduler
        self.storage_service = storage_service
        self.endpoint_manager: EndpointManagerP = endpoint_manager

        # caches ---------------------------------------------------------
        self.inmemory_objects: WeakKeyDictionary[Axo, None] = WeakKeyDictionary()
        self.remote_files: set[str] = set()

        # misc helpers ---------------------------------------------------
        self.import_manager = DefaultImportManager()

        # thread ctl -----------------------------------------------------
        self._running = True
        self.start()

    # ------------------------------------------------------------------ #
    # Persist & fetch helpers
    # ------------------------------------------------------------------ #
    def get_active_object(self, *, bucket_id: str, key: str) -> Result[Axo, Exception]:
        return self.storage_service._get_active_object(bucket_id=bucket_id, key=key)

    def persistify(
        self, instance: Axo, *, bucket_id: str = "axo", key: Optional[str] = None
    ) -> Result[str, Exception]:
        """
        Serialize an active object and store (1) its pickled class‐def, and
        (2) its state bytes.  Uses whatever endpoint manager was injected.
        """
        try:
            key = key or instance.get_axo_key()
            endpoint = self.endpoint_manager.get_endpoint(instance.get_endpoint_id())

            # 1) metadata via endpoint
            meta_res = endpoint.put(key=key, metadata=instance._acx_metadata)
            if meta_res.is_err:
                return Err(meta_res.unwrap_err())

            # 2) class definition
            cls_key_res = self.storage_service.put(
                bucket_id=bucket_id,
                key=f"{key}_class_def",
                tags={
                    "module": instance._acx_metadata.module,
                    "class_name": instance._acx_metadata.class_name,
                },
                data=cp.dumps(instance.__class__),
            )
            if cls_key_res.is_err:
                return Err(cls_key_res.unwrap_err())
            class_def_key = cls_key_res.unwrap()

            # 3) object bytes
            tags = instance._acx_metadata.to_json_with_string_values()
            tags["class_def_key"] = class_def_key
            obj_res = self.storage_service.put(
                bucket_id=bucket_id, key=key, data=instance.to_bytes(), tags=tags
            )
            return obj_res
        except Exception as exc:  # pragma: no cover
            logger.exception("persistify failed")
            return Err(exc)

    # ------------------------------------------------------------------ #
    # Thread lifecycle
    # ------------------------------------------------------------------ #
    @abstractmethod
    def stop(self) -> None: ...

    def run(self) -> None:
        """Main consumer loop."""
        while self._running:
            task: Task = self.q.get()

            if time.time() < task.executes_at:
                self.q.put(task)
                continue

            if task.operation == "PUT":
                self._handle_put_task(task)
            elif task.operation == "DROP":
                logger.debug("DROP not implemented (%s)", task.id)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _handle_put_task(self, task: Task) -> None:
        """Upload file referenced by *task* if not already stored."""
        path = task.metadata.get("path", "")
        if not path or path in self.remote_files:
            return

        res = self.storage_service.put_data_from_file(
            bucket_id=task.metadata.get("bucket_id", "axo"),
            key="",
            source_path=path,
            tags={},
            chunk_size=task.metadata.get("chunk_size", "1MB"),
        )
        if res.is_ok:
            self.remote_files.add(path)
            logger.info("PUT ok %s", path)
        else:
            logger.error("PUT failed %s → %s", path, res.unwrap_err())
