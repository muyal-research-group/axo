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

    def _serialize_attrs(self, attrs) -> tuple[bytes, str]:
        """
        Prefer JSON; fallback to cloudpickle. Returns (bytes, content_type).
        """
        # JSON path
        try:
            b = J.dumps(attrs, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            return b, "application/json"
        except Exception:
            pass

        # cloudpickle fallback
        try:
            import cloudpickle as cp  # local import
            return cp.dumps(attrs), "application/x-python-cloudpickle"
        except Exception as e:
            raise RuntimeError(f"attrs not serializable: {e}")

    def _build_blobs_from_instance(
        self, instance: "Axo", *, bucket_id: str, key: str
    ) -> Result[tuple[AxoObjectBlobs, str], AxoError]:
        """
        Convert an Axo instance into AxoObjectBlobs + class_name.
        Uses instance.get_raw_parts() -> (attrs, class_code_str).
        """
        try:
            raw_parts_res = instance.get_raw_parts()
            if raw_parts_res.is_err:
                return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, str(raw_parts_res.unwrap_err())))
            attrs, class_code_str = raw_parts_res.unwrap()

            # bytes
            src_bytes = class_code_str.encode("utf-8")
            attr_bytes, attr_ct = self._serialize_attrs(attrs)

            # meta
            src_key = SU.source_key(key)
            attr_key = SU.attrs_key(key)

            producer_id = getattr(getattr(instance, "_acx_metadata", None), "producer_id", "") or "axo"
            tags = instance._acx_metadata.to_tags()
            src_md = AxoStorageMetadata(
                key          = src_key,
                ball_id      = src_key,
                size         = len(src_bytes),
                checksum     = SU.sha256_hex(src_bytes),
                producer_id  = producer_id,
                bucket_id    = bucket_id,
                tags         = {**tags},
                content_type = "application/python",
                is_disabled  = False,
            )
            attr_md = AxoStorageMetadata(
                key          = attr_key,
                ball_id      = attr_key,
                size         = len(attr_bytes),
                checksum     = SU.sha256_hex(attr_bytes),
                producer_id  = producer_id,
                bucket_id    = bucket_id,
                tags         = {**tags},
                content_type = attr_ct,
                is_disabled  = False,
            )

            blobs = AxoObjectBlobs(
                source_code_blob=AxoObjectBlob(src_bytes, src_md),
                attrs_blob=AxoObjectBlob(attr_bytes, attr_md),
            )
            class_name = getattr(getattr(instance, "_acx_metadata", None), "axo_class_name", None)
            if not class_name:
                return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, "missing axo_class_name in metadata"))

            return Ok((blobs, class_name))
        except Exception as e:
            return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, f"build_blobs failed: {e}"))


    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    @abstractmethod
    def _handle_put_task(self, task: Task) -> None:
        ...
