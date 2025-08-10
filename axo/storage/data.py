"""
axo/storage/data.py
~~~~~~~~~~~~~~~~~~~

Storage‑service abstractions for Axo.

The module defines:

* :class:`StorageService` – abstract interface every backend must implement
  (put / get, streaming, bucket‑level metadata, etc.).
* :class:`LocalStorageService` – simple local‑filesystem backend for tests or
  single‑node deployments.
* :class:`MictlanXStorageService` – asynchronous backend that wraps the
  MictlanX v4 client.
* Stubs for :class:`AWSS3` and :class:`DropboxStorageService`.

All public APIs return :pyclass:`option.Result` to make error handling explicit.
"""

from __future__ import annotations

import types
import os
import string
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Tuple, TypeVar,TYPE_CHECKING
# 
import cloudpickle as cp
import humanfriendly as hf
from nanoid import generate as nanoid
from option import Err, Ok, Result
from xolo.utils.utils import Utils as XoloUtils

# MictlanX 
# import mictlanx.v4.interfaces as InterfaceX
from mictlanx.utils import Chunks
from mictlanx.utils.index import Utils
from mictlanx.v4.asyncx import AsyncClient
from mictlanx.v4.interfaces import Metadata
# 
if TYPE_CHECKING:
    from axo.core.axo import Axo,axo_method
from axo.log import get_logger
from axo.utils.decorators import guard_if_failed
# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logger = get_logger(name=__name__)

# Typing helpers
T = TypeVar("T")
ChunkIter = Iterator[bytes]

ComposedKey = str
BucketId = str
BallId = str




# ============================================================================
# Abstract base class
# ============================================================================
class StorageService(ABC):
    """
    Common interface for every Axo storage backend (local, S3, MictlanX, …).

    All I/O operations are asynchronous to avoid blocking the event loop.
    """

    def __init__(self, storage_service_id: str) -> None:
        self.storage_service_id = storage_service_id

    # ------------------------------------------------------------------ #
    # CRUD (bytes)
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def put(
        self,
        *,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, Exception]:
        """Upload a full blob (bytes)."""

    @abstractmethod
    async def put_streaming(
        self,
        *,
        bucket_id: str,
        key: str,
        data: ChunkIter,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, Exception]:
        """Upload via iterator/generator of chunks."""

    @abstractmethod
    async def get(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, Exception]:
        """Download entire object into memory."""

    @abstractmethod
    async def get_streaming(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[Tuple[ChunkIter, Dict[str, Any]], Exception]:
        """Stream object back as an iterator plus metadata."""

    # ------------------------------------------------------------------ #
    # Axo‑specific helpers
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def _get_active_object(
        self, *, key: str, bucket_id: str
    ) -> Result[Axo, Exception]:
        """Fetch and deserialize an active object stored by Axo."""

    @abstractmethod
    async def get_bucket_metadata(
        self, bucket_id: str
    ) -> Result[List[Metadata], Exception]:
        """List all metadata entries in a bucket."""

    @abstractmethod
    async def put_data_from_file(
        self,
        *,
        source_path: str,
        key: str,
        bucket_id: str,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[bool, Exception]:
        """Upload a local file directly from disk."""


# ============================================================================
# Local filesystem backend
# ============================================================================
class LocalStorageService(StorageService):
    """
    Very simple backend that stores bytes on the local filesystem under
    ``$AXO_SINK_PATH/<bucket>/<key>``.  Useful for unit tests or single‑node
    deployments.
    """
    def __init__(self, storage_service_id:str, sink_path:str = os.environ.get("AXO_SINK_PATH", "/sink")):
        super().__init__(storage_service_id)
        self.sink_path = f"{sink_path}/axo"
        self.is_failed = False
        try:
            os.makedirs(self.sink_path,exist_ok=True)
        except Exception as e:
            logger.error(str(e))
            self.is_failed = True
        
        self.metadata:Dict[BucketId, Dict[BallId, Metadata]] = {}


    # -- Bucket‑level metadata ------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def get_bucket_metadata(
        self, bucket_id: str
    ) -> Result[List[Metadata], Exception]:
        
        if bucket_id in self.metadata:
            return Ok(list(self.metadata[bucket_id].values()))
        return Err(Exception(f"{bucket_id} not found"))
        # return Err(Exception("get_bucket_metadata not implemented yet"))

    # -- Streaming download ---------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def get_streaming(
        self, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[Tuple[ChunkIter, Dict[str, Any]], Exception]:
        try:
            chunk_size_bytes = hf.parse_size(chunk_size)
            path = f"{self.sink_path}/{bucket_id}/{key}"
            file = open(path, "rb")

            def _iter() -> ChunkIter:
                size = 0
                while True:
                    chunk = file.read(chunk_size_bytes)
                    if not chunk:
                        break
                    size += len(chunk)
                    yield chunk
                file.close()

            return Ok((_iter(), {"path": path}))
        except Exception as exc:
            return Err(exc)

    # -- PUT (bytes) -----------------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def put(
        self,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, Exception]:
        start = time.time()
        key = key or nanoid(alphabet=string.digits + string.ascii_lowercase)
        # sink_path = os.environ.get("AXO_SINK_PATH", "/sink")
        os.makedirs(f"{self.sink_path}/{bucket_id}", exist_ok=True)

        path = f"{self.sink_path}/{bucket_id}/{key}"
        current_checksum = XoloUtils.sha256(data)
        if os.path.exists(path=path):
            (checksum, size) = XoloUtils.sha256_file(path=path)
        else:
            checksum,size = "",0 

        metadata = Metadata(
            key=key,
            size=len(data),
            checksum=current_checksum,
            bucket_id=bucket_id,
            ball_id= key,
            producer_id= "axo",
            tags={},
            content_type= ""
        )
        def __inner():
            with open(path, "wb") as fh:
                fh.write(data)
            logger.debug("PUT.DATA %s %.3fs", path, time.time() - start)
            self.metadata.setdefault(bucket_id, {})
            self.metadata[bucket_id][key] = metadata
            return Ok(path)
        if os.path.exists(path):
            if checksum == current_checksum:
                self.metadata.setdefault(bucket_id,{})
                self.metadata[bucket_id][key] = metadata
                return Ok(path)
            else: 
                return __inner()

        else: 
            return __inner()

    # -- PUT (streaming) -------------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def put_streaming(
        self,
        bucket_id: str,
        key: str,
        data: ChunkIter,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, Exception]:
        try:
            collected = b"".join(data)
            return await self.put(
                bucket_id=bucket_id, key=key, data=collected, tags=tags
            )
        except Exception as exc:
            return Err(exc)

    # -- GET (bytes) -----------------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def get(
        self, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, Exception]:
        try:
            # sink_path = os.environ.get("AXO_SINK_PATH", "/sink")
            path = f"{self.sink_path}/{bucket_id}/{key}"
            with open(path, "rb") as fh:
                return Ok(fh.read())
        except Exception as exc:
            return Err(exc)

    # -- Active‑object helper -------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def _get_active_object(
        self, *, key: str, bucket_id: str
    ) -> Result[Axo, Exception]:
        try:
            path = f"{self.sink_path}/{bucket_id}/{key}"
            with open(path, "rb") as fh:
                return Axo.from_bytes(raw_obj=fh.read())
        except Exception as exc:
            return Err(exc)

    # -- PUT file --------------------------------------------------------
    @guard_if_failed(log = logger.debug)
    async def put_data_from_file(
        self,
        source_path: str,
        key: str = "",
        bucket_id: str = "",
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[bool, Exception]:
        with open(source_path, "rb") as f:
            return await self.put(bucket_id=bucket_id, key=key, data= f.read(),tags=tags,chunk_size=chunk_size)
        # return Err(Exception("put_data_from_file not implemented"))


# ============================================================================
# Placeholder backends
# ============================================================================
class AWSS3(StorageService):
    """TODO: implement S3 backend."""
    pass


class DropboxStorageService(StorageService):
    """TODO: implement Dropbox backend."""
    pass


# ============================================================================
# MictlanX backend
# ============================================================================
class MictlanXStorageService(StorageService):
    """
    Asynchronous storage backend that delegates all I/O to the MictlanX v4
    client library.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        mictlanx_id: str = "axo-mictlanx",
        bucket_id: str = "axo-mictlanx",
        routers_str: str = "mictlanx-router-0:localhost:60666",
        protocol: str = "https",
        max_workers: int = 4,
        log_path: str = "./log",
        client: AsyncClient | None = None,
    ) -> None:
        super().__init__(storage_service_id="MictlanX")

        # Environment overrides
        bucket_id = os.environ.get("MICTLANX_BUCKET_ID", bucket_id)
        routers_str = os.environ.get("MICTLANX_ROUTERS", routers_str)
        protocol = os.environ.get("MICTLANX_PROTOCOL", protocol)

        routers = list(Utils.routers_from_str(routers_str, protocol=protocol))

        self.client: AsyncClient = (
            client
            if client is not None
            else AsyncClient(
                client_id=os.environ.get("MICTLANX_CLIENT_ID", mictlanx_id),
                routers=routers,
                max_workers=int(os.environ.get("MICTLANX_MAX_WORKERS", max_workers)),
                debug=True,
                log_output_path=os.environ.get("MICTLANX_LOG_OUTPUT_PATH", log_path),
                # bucket_id=bucket_id,
            )
        )

    # ------------------------------------------------------------------ #
    # Bucket‑level metadata
    # ------------------------------------------------------------------ #
    async def get_bucket_metadata(
        self, bucket_id: str
    ) -> Result[List[Metadata], Exception]:
        try:
            res = await self.client.get_bucket_metadata(bucket_id=bucket_id)
            if res.is_err:
                return Err(res.unwrap_err())

            unique: Dict[str, List[Metadata]] = {}
            for md in res.unwrap().balls:
                unique.setdefault(md.key, []).append(md)
            return Ok([v[0] for v in unique.values()])
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # PUT (bytes)
    # ------------------------------------------------------------------ #
    async def put(
        self,
        *,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, Exception]:
        try:
            res = await self.client.put(
                bucket_id=bucket_id,
                key=key,
                value=data,
                tags=tags or {},
                chunk_size=chunk_size,
            )
            return Ok(key) if res.is_ok else Err(res.unwrap_err())
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # PUT (streaming)
    # ------------------------------------------------------------------ #
    async def put_streaming(
        self,
        *,
        bucket_id: str,
        key: str,
        data: ChunkIter,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, Exception]:
        try:
            chunks = Chunks.from_generator(
                gen=data, group_id=key, chunk_size=chunk_size
            ).unwrap()
            res = await self.client.put_chunks(
                bucket_id=bucket_id, key=key, chunks=chunks, tags=tags or {}
            )
            return Ok(key) if res.is_ok else Err(res.unwrap_err())
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # GET (streaming)
    # ------------------------------------------------------------------ #
    async def get_streaming(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[Tuple[ChunkIter, Dict[str, Any]], Exception]:
        try:
            res = await self.client.get(
                bucket_id=bucket_id, key=key, chunk_size=chunk_size
            )
            if res.is_err:
                return Err(res.unwrap_err())
            data = res.unwrap().data
            return Ok((data.tobytes(), {}))
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # GET (bytes)
    # ------------------------------------------------------------------ #
    async def get(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, Exception]:
        try:
            res = await self.client.get(
                bucket_id=bucket_id, key=key, chunk_size=chunk_size
            )
            return Ok(res.unwrap().data) if res.is_ok else Err(res.unwrap_err())
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # Active‑object helper
    # ------------------------------------------------------------------ #
    async def _get_active_object(
        self, *, key: str, bucket_id: str
    ) -> Result[Axo, Exception]:
        code_res = await self.client.get(
            bucket_id=bucket_id,
            key=f"{key}_source_code", 
            max_retries=100,
            delay=2,
            chunk_size="1MB"
        )
        attrs_res = await self.client.get(
            bucket_id=bucket_id,
            key=f"{key}_attrs",
            max_retries=100,
            delay=2,
            chunk_size="1MB"
        )
        if code_res.is_err:
            return Err(Exception("Failed to get source code"))
        if attrs_res.is_err:
            return Err(Exception("Failed to get attrs"))
        
        source_code_repsonse       = code_res.unwrap()
        source_code                = cp.loads(source_code_repsonse.data.tobytes())
        attrs_response             = attrs_res.unwrap()
        attrs                      = cp.loads(attrs_response.data.tobytes())
        mod                        = types.ModuleType("__axo_dynamic__")
        mod.__dict__["Axo"]        = Axo
        mod.__dict__["axo_method"] = axo_method
        class_name                 = source_code_repsonse.metadatas[0].tags.get("axo_class_name")
        exec(source_code, mod.__dict__)
        X   = getattr(mod,class_name)
        print("ATTRS", attrs)
        obj = X(**attrs)
        for attr_name, attr_value in attrs.items():
            setattr(obj, attr_name, attr_value) 
        return Ok(obj)

        # res = await self.client.get(bucket_id=bucket_id, key=key)
        # print(res)
        # if res.is_err:
        #     return Err(Exception(f"{key} not found"))
        # response = res.unwrap()
        # data = response.data.tobytes()
        # if data:
        #     return Axo.from_bytes(raw=data)
        # logger.warning({"event": "EMPTY.RESPONSE", "key": key})
        # await asyncio.sleep(1)  # back‑off

    # ------------------------------------------------------------------ #
    # PUT file
    # ------------------------------------------------------------------ #
    async def put_data_from_file(
        self,
        *,
        source_path: str,
        key: str = "",
        bucket_id: str = "",
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[bool, Exception]:
        try:
            res = await self.client.put_file(
                bucket_id=bucket_id,
                key=key,
                path=source_path,
                chunk_size=chunk_size,
                tags=tags or {},
            )
            return Ok(res.is_ok)
        except Exception as exc:
            return Err(exc)

    # Factory helper ----------------------------------------------------
    @staticmethod
    def from_client(client: AsyncClient) -> "MictlanXStorageService":
        """Create service from an existing *synchronous* MictlanX client."""
        return MictlanXStorageService(client=client)
