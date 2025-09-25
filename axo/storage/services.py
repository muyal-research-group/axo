from __future__ import annotations

import os
import time as T
from typing import Dict,Optional,List
import tempfile
from functools import reduce

from nanoid import generate as nanoid
from option import Err, Ok, Result
import json as J
# MictlanX 
from mictlanx.utils.index import Utils
from mictlanx import AsyncClient
# 
from axo.log import get_logger
from axo.storage.types import StorageService,BucketId,BallId,AxoStorageMetadata
from axo.storage.utils import StorageUtils as SU
from axo.errors import AxoError,AxoErrorType
# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logger = get_logger(name=__name__)




# ============================================================================
# Local filesystem backend
# ============================================================================


class LocalStorageService(StorageService):
    """
    Simple local‑filesystem backend.

    Layout (under `sink_path`, default: $AXO_SINK_PATH or "/axo"):
      - Data:     {sink_path}/{bucket_id}/{key}
      - Metadata: {sink_path}/metadata/{bucket_id}/{key}.json

    Notes
    -----
    * Writes are atomic: data written to a temp file then renamed.
    * Re‑PUT with identical content (same checksum) does NOT rewrite the file.
    * Metadata persisted as AxoStorageMetadata JSON (one file per object).
    """

    def __init__(
        self,
        storage_service_id: str = "local",
        sink_path: str = os.environ.get("AXO_SINK_PATH", "/axo"),
    ):
        super().__init__(storage_service_id)
        self.sink_path = sink_path
        self.data_root = self.sink_path  # data root
        self.meta_root = os.path.join(self.sink_path, "metadata")
        self.is_failed = False

        try:
            os.makedirs(self.data_root, exist_ok=True)
            os.makedirs(self.meta_root, exist_ok=True)
        except Exception as e:
            logger.error("LocalStorageService init error: %s", e)
            self.is_failed = True

        # In‑memory cache (optional; source of truth is the JSON files)
        self._meta: Dict[BucketId, Dict[BallId, AxoStorageMetadata]] = {}

    # --------------------------- path helpers ---------------------------

    def _data_dir(self, bucket_id: str) -> str:
        return os.path.join(self.data_root, bucket_id)

    def _data_path(self, bucket_id: str, key: str) -> str:
        return os.path.join(self._data_dir(bucket_id), key)

    def _meta_dir(self, bucket_id: str) -> str:
        return os.path.join(self.meta_root, bucket_id)

    def _meta_path(self, bucket_id: str, key: str) -> str:
        return os.path.join(self._meta_dir(bucket_id), f"{key}.json")

    def _cache_get(self, bucket_id: str, key: str) -> AxoStorageMetadata | None:
        return self._meta.get(bucket_id, {}).get(key)

    def _cache_put(self, md: AxoStorageMetadata) -> None:
        self._meta.setdefault(md.bucket_id, {})
        self._meta[md.bucket_id][md.key] = md

    def _cache_del(self, bucket_id: str, key: str) -> None:
        if bucket_id in self._meta and key in self._meta[bucket_id]:
            del self._meta[bucket_id][key]

    # ------------------------------- CRUD -------------------------------

    async def delete(self, *, bucket_id: str, key: str) -> Result[bool, AxoError]:

        """Delete data file and its metadata JSON (if exist)."""
        try:
            removed = False
            dpath = self._data_path(bucket_id, key)
            mpath = self._meta_path(bucket_id, key)

            if os.path.exists(dpath):
                os.remove(dpath)
                removed = True

            if os.path.exists(mpath):
                os.remove(mpath)
                removed = True

            self._cache_del(bucket_id, key)
            return Ok(removed)
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.DELETE_FAILED,msg=str(e))
            return Err(_e)

    async def put(
        self,
        *,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, AxoError]:
        """
        Atomic PUT. If the existing file's checksum matches the new data,
        metadata is refreshed and the file is NOT rewritten.
        """
        t0   = T.time()
        key  = key or nanoid()
        ddir = self._data_dir(bucket_id)
        mdir = self._meta_dir(bucket_id)
        os.makedirs(ddir, exist_ok=True)
        os.makedirs(mdir, exist_ok=True)

        dpath = self._data_path(bucket_id, key)
        mpath = self._meta_path(bucket_id, key)
        _tags = (tags or {})

        # Prepare metadata
        current_checksum = SU.sha256_hex(data)
        content_type     = _tags.get("content_type", "application/octet-stream")
        md = AxoStorageMetadata(
            key          = key,
            ball_id      = _tags.get("ball_id", key),
            size         = len(data),
            checksum     = current_checksum,
            producer_id  = _tags.get("producer_id", ""),
            bucket_id    = bucket_id,
            tags         = {**_tags},
            content_type = content_type,
            is_disabled  = False,
        )

        # If file exists and checksum matches, just (re)persist metadata
        if os.path.exists(dpath):
            try:
                # compute existing checksum
                with open(dpath, "rb") as f:
                    existing = f.read()
                

                if SU.sha256_hex(existing) == current_checksum:
                    self._persist_metadata(mpath, md)
                    self._cache_put(md)
                    logger.debug("PUT.SKIP %s (same checksum) %.3fs", dpath, T.time() - t0)
                    return Ok(dpath)
            except Exception as e:
                # fall through to rewrite path on errors
                logger.warning("PUT: checksum check failed, will rewrite: %s", e)

        # Write atomically: tmp → rename
        try:
            fd, tmp_path = tempfile.mkstemp(dir=ddir)
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(data)
                os.replace(tmp_path, dpath)  # atomic on same filesystem
            finally:
                # if something went wrong before replace
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass

        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.PUT_DATA_FAILED,msg=str(e))
            return Err(_e)

        # Persist metadata JSON
        try:
            self._persist_metadata(mpath, md)
            self._cache_put(md)
        except Exception as e:
            # best‑effort cleanup if metadata write fails
            try:
                if os.path.exists(dpath):
                    os.remove(dpath)
            finally:
                _e = AxoError.make(error_type=AxoErrorType.PUT_METADATA_FAILED,msg=str(e))
                return Err(_e)

        logger.debug("PUT.DATA %s %.3fs", dpath, T.time() - t0)
        return Ok(dpath)

    async def get(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, AxoError]:
        """Read full bytes into memory."""
        try:
            dpath = self._data_path(bucket_id, key)
            
            if not os.path.exists(dpath):
                _e = AxoError.make(error_type=AxoErrorType.NOT_FOUND,msg=str(e))
                return Err(_e)
            
            with open(dpath, "rb") as fh:
                return Ok(fh.read())
            
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.GET_DATA_FAILED,msg=str(e))
            return Err(_e)
            # return Err(e)

    async def put_data_from_file(
        self,
        *,
        source_path: str,
        key: str = "",
        bucket_id: str = "",
        tags: Dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[bool, AxoError]:
        """Read a local file and forward to PUT."""
        try:

            with open(source_path, "rb") as f:
                data = f.read()
            res = await self.put(
                bucket_id=bucket_id, key=key, data=data, tags=tags or {}, chunk_size=chunk_size
            )
            return Ok(True) if res.is_ok else Err(res.unwrap_err())
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.PUT_DATA_FAILED,msg=str(e))
            return Err(_e)

    # -------------------------- metadata API ----------------------------

    async def get_metadata(
        self, bucket_id: str, key: str
    ) -> Result[AxoStorageMetadata, AxoError]:
        """Load per‑object metadata from cache or JSON file."""
        try:
            cached = self._cache_get(bucket_id, key)
            if cached:
                return Ok(cached)

            mpath = self._meta_path(bucket_id, key)
            with open(mpath, "r", encoding="utf-8") as fh:
                raw = J.load(fh)
            md = AxoStorageMetadata(**raw)
            self._cache_put(md)
            return Ok(md)
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.GET_METADATA_FAILED,msg=str(e))
            return Err(_e)

    # ---------------------------- internals -----------------------------

    def _persist_metadata(self, mpath: str, md: AxoStorageMetadata) -> None:
        """Atomic metadata write (tmp → rename)."""
        os.makedirs(os.path.dirname(mpath), exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(mpath))
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                J.dump(md.model_dump(), fh, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp_path, mpath)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass



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
    Asynchronous storage backend that delegates I/O to the MictlanX v4 client.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        mictlanx_id: str = "axo-mictlanx",
        bucket_id: str = "axo-mictlanx",
        uri: str = "mictlanx://mictlanx-router-0@localhost:60666?/api_version=4&protocol=http",
        # protocol: str = "https",
        max_workers: int = 4,
        log_path: str = "/log",
        debug: bool = True,
        client: Optional[AsyncClient] = None,
    ) -> None:
        super().__init__(storage_service_id="MictlanX")

        # Environment overrides
        self.default_bucket_id = os.environ.get("MICTLANX_BUCKET_ID", bucket_id)
        # routers_str            = os.environ.get("MICTLANX_ROUTERS", routers_str)
        # protocol               = os.environ.get("MICTLANX_PROTOCOL", protocol)
        # routers                = list(Utils.routers_from_str(routers_str, protocol=protocol))

        self.client: AsyncClient = (
            client
            if client is not None
            else AsyncClient(
                uri             = uri,
                client_id       = os.environ.get("MICTLANX_CLIENT_ID", mictlanx_id),
                max_workers     = int(os.environ.get("MICTLANX_MAX_WORKERS", max_workers)),
                debug           = debug,
                log_output_path = os.environ.get("MICTLANX_LOG_OUTPUT_PATH", log_path),
            )
        )

    # ------------------------------ Helpers ----------------------------- #
    @staticmethod
    def merge_dicts_list(dicts: List[Dict[str, str]]) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        for d in dicts:
            if d:
                merged.update(d)
        return merged

    @staticmethod
    def _mk_axo_error(default_type: AxoErrorType, err: Exception | str) -> AxoError:
        s = str(err)
        st = default_type
        low = s.lower()
        if "not found" in low or "404" in low or "no such key" in low:
            st = AxoErrorType.NOT_FOUND
        elif "timeout" in low or "timed out" in low:
            st = AxoErrorType.TIMEOUT
        elif "connection" in low or "transport" in low or "network" in low:
            st = AxoErrorType.TRANSPORT_ERROR
        return AxoError.make(error_type=st, msg=s)
    # ------------------------------------------------------------------ #


    async def put(
        self,
        *,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: Dict[str, str] = {},
        chunk_size: str = "1MB",
    ) -> Result[str, AxoError]:
        try:
            key = key or nanoid()

            res = await self.client.put(
                bucket_id  = bucket_id or self.default_bucket_id,
                key        = key,
                value      = data,
                tags       = tags or {},
                chunk_size = chunk_size,
            )
            if res.is_ok:
                return Ok(key)
            return Err(self._mk_axo_error(AxoErrorType.PUT_DATA_FAILED, res.unwrap_err()))
        except Exception as e:
            return Err(self._mk_axo_error(AxoErrorType.TRANSPORT_ERROR, e))

    async def get(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, AxoError]:
        try:
            res = await self.client.get(
                bucket_id=bucket_id or self.default_bucket_id,
                key=key,
                chunk_size=chunk_size,
            )
            if res.is_ok:
                return Ok(res.unwrap().data.tobytes())
            me = res.unwrap_err()
            et = AxoErrorType.GET_DATA_FAILED
            if me.status_code == 404:
                et = AxoErrorType.NOT_FOUND
            return Err(self._mk_axo_error(et, me ))
        except Exception as e:
            return Err(self._mk_axo_error(AxoErrorType.TRANSPORT_ERROR, e))

    async def delete(self, *, bucket_id: str, key: str) -> Result[bool, AxoError]:
        try:
            res = await self.client.delete(
                bucket_id=bucket_id or self.default_bucket_id,
                ball_id=key,
            )
            if res.is_ok:
                return Ok(True)
            return Err(self._mk_axo_error(AxoErrorType.DELETE_FAILED, res.unwrap_err()))
        except Exception as e:
            return Err(self._mk_axo_error(AxoErrorType.TRANSPORT_ERROR, e))

    async def put_data_from_file(
        self,
        *,
        source_path: str,
        key: str = "",
        bucket_id: str = "",
        tags: Dict[str, str] = {},
        chunk_size: str = "1MB",
    ) -> Result[bool, AxoError]:
        try:
            res = await self.client.put_file(
                bucket_id  = bucket_id or self.default_bucket_id,
                key        = key or nanoid(),
                path       = source_path,
                tags       = tags or {},
                chunk_size = chunk_size,
            )
            if res.is_ok:
                return Ok(True)
            return Err(self._mk_axo_error(AxoErrorType.PUT_DATA_FAILED, res.unwrap_err()))
        except Exception as e:
            return Err(self._mk_axo_error(AxoErrorType.TRANSPORT_ERROR, e))

    # -------------------------- metadata API ---------------------------- #

    async def get_metadata(self, bucket_id: str, key: str) -> Result[AxoStorageMetadata, AxoError]:
        """
        Try a lightweight metadata call if the client supports it; otherwise
        fall back to `get` and read the metadata from the response object.
        """
        # 1) Preferred: dedicated metadata/head call (if available)
        try:
            r = await self.client.get_metadata(
                bucket_id = bucket_id or self.default_bucket_id,
                ball_id   = key,
            )
            # if r.is_ok:
            if r.is_err:
                me = r.unwrap_err()
                et = AxoErrorType.GET_METADATA_FAILED
                if me.status_code == 404:
                    et = AxoErrorType.NOT_FOUND
                return Err(self._mk_axo_error(et, me))
            
            mm    = r.unwrap()
            dicts = map(lambda x:x.tags,mm.chunks)
            tags  = MictlanXStorageService.merge_dicts_list(dicts)
            # reduce(lambda x: self.merge_dicts(),dicts)
            asm = AxoStorageMetadata(
                key          = key,
                bucket_id    = bucket_id,
                ball_id      = mm.ball_id,
                checksum     = mm.checksum,
                content_type = "application/octet-stream",
                size         = mm.size,
                is_disabled  = False,
                producer_id  = mm.chunks[0].producer_id,
                tags         = tags,
            )
            return Ok(asm)
        except Exception as e:
            return Err(self._mk_axo_error(AxoErrorType.TRANSPORT_ERROR, e))

    # ------------------------------- Factory ---------------------------- #

    @staticmethod
    def from_client(client: AsyncClient) -> "MictlanXStorageService":
        """Create service from an existing AsyncClient."""
        return MictlanXStorageService(client=client)




# =========================
# in-memory backend
# =========================
class InMemoryStorageService(StorageService):
    def __init__(self, storage_service_id: str = "in-memory-0") -> None:
        super().__init__(storage_service_id)
        # buckets[bucket_id][key] = bytes
        self.buckets: dict[str, dict[str, bytes]] = {}
        # meta[bucket_id][key] = AxoStorageMetadata
        self.meta: dict[str, dict[str, AxoStorageMetadata]] = {}

    async def put(
        self,
        *,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: dict[str, str] | None = None,
        chunk_size: str = "1MB",
    ) -> Result[str, AxoError]:
        self.buckets.setdefault(bucket_id, {})
        self.meta.setdefault(bucket_id, {})

        # Persist bytes

        # Reconstruct AxoStorageMetadata from tags
        tags = tags or {}
        try:
            md = AxoStorageMetadata(
                key=key,
                ball_id=tags.get("ball_id", key),
                size=int(tags.get("size", len(data))),
                checksum=tags.get("checksum", SU.sha256_hex(data)),
                producer_id=tags.get("producer_id", ""),
                bucket_id=bucket_id,
                tags={k: v for k, v in tags.items()},  # keep all tags
                content_type=tags.get("content_type", "application/octet-stream"),
                is_disabled=False,
            )
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.VALIDATION_FAILED,msg=str(e))
            return Err(_e)


        try:
            self.buckets[bucket_id][key] = data
            self.meta[bucket_id][key] = md
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.STORAGE_ERROR,msg=str(e))
            return Err(_e)

        return Ok(key)

 

    async def get(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, AxoError]:
        try:
            b = self.buckets[bucket_id][key]
            return Ok(b)
        except KeyError as e:
            _e = AxoError.make(error_type=AxoErrorType.NOT_FOUND, msg=str(f"{bucket_id}@{key} not found"))
            return Err(_e)
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.STORAGE_ERROR,msg=str(e))
            return Err(_e)

            # return Err(Exception("not found"))



    async def delete(self, *, bucket_id: str, key: str) -> Result[bool, AxoError]:
        try: 
            existed = False
            if bucket_id in self.buckets and key in self.buckets[bucket_id]:
                existed = True
                del self.buckets[bucket_id][key]
            if bucket_id in self.meta and key in self.meta[bucket_id]:
                del self.meta[bucket_id][key]
            return Ok(existed)
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.STORAGE_ERROR,msg=str(e))
            return Err(_e)

    # NEW API: per-object metadata
    async def get_metadata(
        self, bucket_id: str, key: str
    ) -> Result[AxoStorageMetadata, AxoError]:
        try:
            return Ok(self.meta[bucket_id][key])
        except KeyError:
            _e = AxoError.make(error_type=AxoErrorType.NOT_FOUND, msg=str(f"{bucket_id}@{key} not found"))
            return Err(_e)
        except Exception as e:
            _e = AxoError.make(error_type=AxoErrorType.STORAGE_ERROR,msg=str(e))
            return Err(_e)

    async def put_data_from_file(
        self, *, source_path: str, key: str, bucket_id: str, tags=None, chunk_size: str = "1MB"
    )->Result[bool, AxoError]:
        try:
            with open(source_path, "rb") as fh:
                data = fh.read()
            res = await self.put(bucket_id=bucket_id, key=key, data=data, tags=tags or {}, chunk_size=chunk_size)
            return Ok(True) if res.is_ok else Err(res.unwrap_err())
        except Exception as e: 
            _e = AxoError.make(error_type=AxoErrorType.STORAGE_ERROR,msg=str(e))
            return Err(_e)