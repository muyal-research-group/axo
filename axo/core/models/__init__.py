
import humanfriendly as HF
from pydantic import BaseModel,Field,field_validator,ConfigDict,model_validator
from typing import ClassVar,Optional,List,Dict,Any,Tuple,Literal,Union,Iterable
import os
import re
from dataclasses import dataclass
# 
from axo.helpers import _generate_id,_build_axo_uri
from option import Ok, Err, Result
from axo.errors import AxoError, AxoErrorType
from axo.storage.services import StorageService
from axo.storage.types import AxoStorageMetadata 

# pydantic *Annotated* alias for object keys
# =========================================================================== #
# Metadata (pydantic model)
# =========================================================================== #
AXO_ID_SIZE = int(os.getenv("AXO_ID_SIZE", "8"))

_AXO_URI_RE = re.compile(r"^axo://(?P<bucket>[^:]+):(?P<key>[^/]+)(?:/(?P<ver>\d+))?$")

# def _gen_id(val: Optional[str] = None, *, size: int = AXO_ID_SIZE) -> str:
#     return _generate_id(val=val, size=size)

def _norm(s: str) -> str:
    return s.strip()

def _norm_ident(s: str) -> str:
    # conservative sanitizer for module/name/class
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", s.strip())

# def _build_axo_uri(bucket: str, key: str, version: int) -> str:
#     return f"axo://{bucket}:{key}/{version}"
def _norm_strip(v: Optional[str]) -> Optional[str]:
    """Remove leading/trailing whitespace and all spaces inside."""
    if v is None:
        return None
    return "".join(v.split())  # removes *all* spaces
class MetadataX(BaseModel):
    """Serializable metadata stored alongside every active object."""

    # Class-level defaults (paths can be overridden via env-vars)
    path: ClassVar[str]        = os.getenv("ACTIVE_LOCAL_PATH", "/axo/data")
    source_path: ClassVar[str] = os.getenv("AXO_SOURCE_PATH", "/axo/source")
    sink_path: ClassVar[str]   = os.getenv("AXO_SINK_PATH", "/axo/sink")

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    # Stored fields ------------------------------------------------------------
    axo_is_read_only: bool = False

    axo_key:str = Field(default_factory=lambda: _generate_id(None))
    axo_bucket_id: str = Field(default_factory=lambda: _generate_id(None))
    axo_source_bucket_id: str =  Field(default_factory=lambda: _generate_id(None))
    axo_sink_bucket_id: str = Field(default_factory=lambda: _generate_id(None))
    axo_module: str
    # axo_name: str
    axo_class_name: str
    axo_version: int = 0  # non-negative; see validator


    axo_endpoint_id: Optional[str] = None
    axo_dependencies: List[str] = Field(default_factory=list)

    # Derived / convenience (kept as stored fields for backward-compat)
    axo_uri: Optional[str] = None
    axo_alias: Optional[str] = None

    # ------------------------------ Validators -------------------------------

    @field_validator("axo_dependencies", mode="before")
    @classmethod
    def _v_strip_deps(cls, v: List[str]) -> List[str]:
        return [_norm_strip(d) for d in v] if v else []

    
    @field_validator("axo_version")
    @classmethod
    def _v_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("axo_version must be >= 0")
        return v

    @field_validator("axo_module",  "axo_class_name")
    @classmethod
    def _v_non_empty_norm(cls, v: str) -> str:
        v = _norm(v)
        if not v:
            raise ValueError("field cannot be empty")
        return _norm_ident(v)

    @field_validator("axo_dependencies", mode="after")
    @classmethod
    def _v_deps_unique(cls, v: List[str]) -> List[str]:
        # normalize + deduplicate while preserving order
        seen = set()
        out: List[str] = []
        for d in v or []:
            d2 = _norm_ident(d)
            if d2 and d2 not in seen:
                seen.add(d2)
                out.append(d2)
        return out

    
    @field_validator("axo_module", "axo_class_name", "axo_key",
                     "axo_bucket_id", "axo_source_bucket_id",
                     "axo_sink_bucket_id", "axo_endpoint_id", "axo_alias",
                     mode="before")
    @classmethod
    def _v_strip_spaces(cls, v: Optional[str]) -> Optional[str]:
        """Remove leading/trailing whitespace and all spaces inside."""
        return _norm_strip(v)
    @model_validator(mode="after")
    def _fill_alias_and_uri(self) -> "MetadataX":
        if not self.axo_alias:
            object.__setattr__(self, "axo_alias", str(self.axo_key))

        # Always (re)build URI
        uri = _build_axo_uri(
            axo_bucket_id=self.axo_bucket_id,
            axo_key=self.axo_key,
            axo_version=self.axo_version,
            class_name=self.axo_class_name,
            method       =None,  # or self.axo_method if you add one
        )
        object.__setattr__(self, "axo_uri", uri)

        return self
    # @model_validator(mode="after")
    # def _fill_ids_and_uri(self) -> "MetadataX":
    #     # IDs: only generate when missing/empty
    #     # if not self.axo
    #     # if self.axo
    #     if self.axo_key is None or self.axo_key == "":
    #         self.axo_key              = _generate_id(self.axo_key)
    #     if self.axo_bucket_id is None or self.axo_bucket_id == "":
    #         self.axo_bucket_id        = _generate_id(self.axo_bucket_id,        size=AXO_ID_SIZE * 2)

    #     if self.axo_sink_bucket_id is None or self.axo_sink_bucket_id =="":
    #         self.axo_sink_bucket_id   = _generate_id(self.axo_sink_bucket_id,   size=AXO_ID_SIZE * 2)
    #     if self.axo_source_bucket_id is None or self.axo_source_bucket_id =="":
    #         self.axo_source_bucket_id = _generate_id(self.axo_source_bucket_id, size=AXO_ID_SIZE * 2)

    #     # Alias (simple, stable)
    #     if self.axo_alias is None or self.axo_alias =="":
    #         self.axo_alias = self.axo_alias or str(self.axo_key)

    #     # axo_uri: if caller provided one, trust but validate; else build
    #     if self.axo_uri:
    #         b, k, v = self._parse_axo_uri(self.axo_uri)
    #         # if uri conflicts with fields, prefer explicit fields and rebuild
    #         if b != self.axo_bucket_id or k != self.axo_key or (v is not None and v != self.axo_version):
    #             self.axo_uri = _build_axo_uri(self.axo_bucket_id, self.axo_key, self.axo_version)
    #     else:
    #         self.axo_uri = _build_axo_uri(self.axo_bucket_id, self.axo_key, self.axo_version)
    #     return self

    # ------------------------------- Methods ---------------------------------

    def rebuild_uri(self) -> None:
        """Recompute axo_uri from bucket/key/version."""
        self.axo_uri = _build_axo_uri(self.axo_bucket_id, self.axo_key, self.axo_version)

    def bump_version(self, *, to: Optional[int] = None, delta: int = 1) -> "MetadataX":
        """
        Return a **new MetadataX** with version bumped (or set to `to`) and URI rebuilt.
        Does not mutate the original instance.
        """
        new_ver = to if to is not None else (self.axo_version + delta)
        if new_ver < 0:
            raise ValueError("new version must be >= 0")
        copy = self.model_copy(update={"axo_version": new_ver})
        copy.rebuild_uri()
        return copy

    def with_endpoint(self, endpoint_id: str) -> "MetadataX":
        """Return a **new** instance with a different endpoint id."""
        return self.model_copy(update={"axo_endpoint_id": _norm_ident(endpoint_id)})

    @staticmethod
    def _parse_axo_uri(uri: str) -> Tuple[str, str, Optional[int]]:
        """
        Parse axo://<bucket>:<key>[/<version>] -> (bucket, key, version?)
        Raises ValueError if invalid.
        """
        m = _AXO_URI_RE.match(uri.strip())
        if not m:
            raise ValueError(f"Invalid axo_uri: {uri!r}")
        bucket = m.group("bucket")
        key    = m.group("key")
        ver_s  = m.group("ver")
        ver    = int(ver_s) if ver_s is not None else None
        return bucket, key, ver

    @classmethod
    def from_axo_uri(cls, *, axo_uri: str, **kwargs: Any) -> "MetadataX":
        """
        Convenience factory to create a minimal MetadataX from a uri and extra fields.
        You still must provide module/name/class_name in kwargs.
        """
        bucket, key, ver = cls._parse_axo_uri(axo_uri)
        base = dict(
            axo_bucket_id=bucket,
            axo_key=key,
            axo_version=(ver if ver is not None else 0),
            axo_uri=axo_uri,
        )
        return cls(**{**base, **kwargs})

    def to_tags(self, *, sep: str = ";") -> Dict[str, str]:
        """
        Return a dict suitable for object store tags (all values are strings).
        Lists are joined with `sep`.
        """
        out: Dict[str, str] = {}
        for k, v in self.model_dump().items():
            if isinstance(v, list):
                out[k] = sep.join(map(str, v))
            elif v is None:
                out[k] = ""
            else:
                out[k] = str(v)
        return out


DeserializeT = Literal["bytes", "json", "pickle"]
AckT         = Union[Literal["delete"],Dict[str, Dict[str, str]]] 

@dataclass(frozen=True)
class AxoContext:
    kind: Optional[Literal["method","task", "stream"]] = "task"
    source_bucket: Optional[str] = ""
    sink_bucket: Optional[str] = ""

    # Selection
    filter_tags: Optional[Dict[str, str]] = None   # AND of tags (k=v)
    filter_prefix: Optional[str] = None            # optional key prefix

    # Data handling
    deserialize: DeserializeT = "bytes"            # bytes | json | pickle

    # Processing semantics
    ack: AckT = "delete"                           # delete | tag-merge
    lease_seconds: int = 60                        # visibility / lease TTL

    # Stream-only knobs
    parallel: Optional[int] = 1                              # in-endpoint concurrency
    batch: Optional[int] = 16                                # max items per poll/iteration
    max_items: Optional[int] = None                # None => unlimited
    max_seconds: Optional[int] = None              # time budget per call
    ignore_ss:Optional[bool] = False # Ignore source and sink  this take the values of the active object



# axo/materialize.py

class ChunkRef(BaseModel):
    """Immutable reference to one chunk of a Ball."""
    model_config = ConfigDict(frozen=True)

    index: int = Field(ge=0, description="Zero-based chunk index")
    size: int = Field(ge=0, description="Chunk size in bytes")
    checksum: str = Field(..., description="Chunk checksum (e.g. SHA-256 hex)")
    tags: Dict[str, str] = Field(default_factory=dict)

    @field_validator("checksum")
    @classmethod
    def _hexish(cls, v: str) -> str:
        # Best-effort guard; do not over-constrain if backend uses other encodings
        if not v:
            return v
        hv = v.lower()
        if all(c in "0123456789abcdef" for c in hv):
            return v
        # Allow non-hex checksums too; just strip whitespace
        return v.strip()


class BallRef(BaseModel):
    """
    Immutable reference to a stored object ("Ball") which may have N chunks.
    Designed for JSON round-trips and forward-compatibility.
    """
    model_config = ConfigDict(frozen=True)

    # optional schema/version for future migrations
    v: int = Field(default=1, description="Schema version")

    bucket_id: str
    key: str
    ball_id: str  # often == key
    size: int = Field(ge=0)
    checksum: str
    content_type: str = Field(default="application/octet-stream")
    tags: Dict[str, str] = Field(default_factory=dict)
    chunks: Tuple[ChunkRef, ...] = Field(default_factory=tuple)

    # ---------- JSON helpers ----------

    def to_json(self, **kwargs) -> str:
        """Compact JSON by default; override with kwargs as needed."""
        if "exclude_none" not in kwargs:
            kwargs["exclude_none"] = True
        if "by_alias" not in kwargs:
            kwargs["by_alias"] = True
        return self.model_dump_json(**kwargs)

    @classmethod
    def from_json(cls, s: str) -> "BallRef":
        return cls.model_validate_json(s)

    # ---------- Constructors from backends ----------

    @classmethod
    def from_metadata(
        cls,
        md: AxoStorageMetadata,
        *,
        chunk_tags: Optional[Dict[str, str]] = None,
    ) -> "BallRef":
        """
        Build a BallRef from AxoStorageMetadata (InMemory/Local).
        Produces a single logical chunk covering the full object.
        """
        ctags = dict(chunk_tags or {})
        # prefer md.tags if available for chunk too
        if md.tags:
            ctags.update(md.tags)

        one = ChunkRef(
            index=0,
            size=int(md.size),
            checksum=md.checksum,
            tags=ctags,
        )
        return cls(
            bucket_id=md.bucket_id,
            key=md.key,
            ball_id=md.ball_id or md.key,
            size=int(md.size),
            checksum=md.checksum,
            content_type=md.content_type or "application/octet-stream",
            tags=dict(md.tags or {}),
            chunks=(one,),
        )
    
    def to_pointer(self, storage, consume: bool = False, delete_remote: bool = False) -> 'AxoPointer':
        """Creates an AxoPointer from this BallRef instance."""
        return AxoPointer(storage, self, consume=consume, delete_remote=delete_remote)

@dataclass(frozen=True)
class AxoPointerState:
    """Pequeño contenedor de flags de ciclo de vida."""
    consume: bool           # one-shot: invalida la ref tras materializar
    delete_remote: bool     # borrar en el backend tras materializar


class AxoPointer:
    """
    A Pointer to a remote Axo object (Ball), with methods to materialize it.
    """
    __slots__ = ("_storage", "_ref", "_state", "_alive")

    def __init__(
        self,
        storage: StorageService,
        ref: BallRef,
        *,
        consume: bool = False,
        delete_remote: bool = False,
    ) -> None:
        self._storage = storage
        self._ref = ref
        self._state = AxoPointerState(consume=consume, delete_remote=delete_remote)
        self._alive = True

    # --------- helpers internos ---------
    def _ensure_alive(self) -> Optional[Result[None, AxoError]]:
        if not self._alive:
            return Err(AxoError.make(AxoErrorType.BAD_REQUEST, "pointer already consumed/destroyed"))
        return None

    async def _maybe_delete_remote(self) -> Result[bool, AxoError]:
        if self._state.delete_remote:
            res = await self._storage.delete(bucket_id=self._ref.bucket_id, key=self._ref.key)
            if res.is_err:
                return Err(AxoError.make(AxoErrorType.DELETE_FAILED, str(res.unwrap_err())))
            return Ok(bool(res.unwrap()))
        return Ok(False)

    def _maybe_consume(self) -> None:
        if self._state.consume:
            self._alive = False

    # --------- API pública de materialización ---------

    async def into_bytes(self, chunk_size: str = "4MB") -> Result[bytes, AxoError]:
        """Unifica todos los chunks y devuelve bytes (no escribe a disco)."""
        err = self._ensure_alive()
        if err is not None:
            return err  # type: ignore[return-value]

        # Usa get_streaming si existe; si no, get() completo
        # get_streaming = self._storage.get()
        # getattr(self._storage, "get_streaming", None)
        # if callable(get_streaming):
        #     r = await get_streaming(self._ref.bucket_id, self._ref.key, chunk_size=chunk_size)
        #     if r.is_err:
        #         return Err(AxoError.make(AxoErrorType.GET_DATA_FAILED, str(r.unwrap_err())))
        #     it, _info = r.unwrap()
        #     data = b"".join(it)
        # else:
        r = await self._storage.get(bucket_id=self._ref.bucket_id, key=self._ref.key, chunk_size=chunk_size)
        if r.is_err:
            return Err(AxoError.make(AxoErrorType.GET_DATA_FAILED, str(r.unwrap_err())))
        data = r.unwrap()

        # post-acciones
        del_res = await self._maybe_delete_remote()
        if del_res.is_err:
            return del_res  # devuelve el error de borrado

        self._maybe_consume()
        return Ok(data)

    @staticmethod
    def validate_bytes(max_bytes:Union[str,int]):
        try:
            if isinstance(max_bytes, str):
                limit = int(HF.parse_size(max_bytes))
            elif isinstance(max_bytes, int):
                limit = max_bytes
            else:
                return Err(AxoError.make(
                    AxoErrorType.VALIDATION_FAILED,
                    f"max_bytes must be int or str, got {type(max_bytes).__name__}",
                ))
            if limit < 0:
                return Err(AxoError.make(
                    AxoErrorType.VALIDATION_FAILED,
                    "max_bytes must be >= 0",
                ))
            else:
                return Ok(limit)
        except Exception as e:
            return Err(AxoError.make(
                AxoErrorType.VALIDATION_FAILED,
                f"invalid max_bytes='{max_bytes}': {e}",
            ))
    async def as_memoryview(
        self,
        max_bytes: Union[int,str] = "256kb",
        chunk_size: str = "4MB",
    ) -> Result[memoryview, AxoError]:
        """Igual que into_bytes(), pero devuelve memoryview (con límite opcional)."""
        r = await self.into_bytes(chunk_size=chunk_size)
        if r.is_err:
            return r  # bubble-up
        data = r.unwrap()

        _mb = AxoPointer.validate_bytes(max_bytes=max_bytes).unwrap_or(0)
        if len(data) > _mb:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, f"object too large for memoryview: {len(data)} bytes"))
        return Ok(memoryview(data))

    async def into_file(self, dest_path: str, chunk_size: str = "4MB", overwrite: bool = True) -> Result[str, AxoError]:
        """Vuelca por streaming a un archivo en `dest_path`."""
        err = self._ensure_alive()
        if err is not None:
            return err  # type: ignore[return-value]

        d = os.path.dirname(dest_path) or "."
        os.makedirs(d, exist_ok=True)
        if os.path.exists(dest_path) and not overwrite:
            return Err(AxoError.make(AxoErrorType.ALREADY_EXISTS, f"path exists: {dest_path}"))

        get_streaming = getattr(self._storage, "get_streaming", None)
        if callable(get_streaming):
            r = await get_streaming(self._ref.bucket_id, self._ref.key, chunk_size=chunk_size)
            if r.is_err:
                return Err(AxoError.make(AxoErrorType.GET_DATA_FAILED, str(r.unwrap_err())))
            it, _info = r.unwrap()
            with open(dest_path, "wb") as fh:
                for chunk in it:
                    fh.write(chunk)
        else:
            r = await self._storage.get(bucket_id=self._ref.bucket_id, key=self._ref.key, chunk_size=chunk_size)
            if r.is_err:
                return Err(AxoError.make(AxoErrorType.GET_DATA_FAILED, str(r.unwrap_err())))
            with open(dest_path, "wb") as fh:
                fh.write(r.unwrap())

        # post-acciones
        del_res = await self._maybe_delete_remote()
        if del_res.is_err:
            return del_res
        self._maybe_consume()
        return Ok(dest_path)

    # --------- metadatos y ciclo de vida ---------

    @property
    def meta(self) -> BallRef:
        """Acceso de solo lectura a la metadata del Ball."""
        return self._ref

    def is_alive(self) -> bool:
        return self._alive

    def destroy(self) -> None:
        """Invalida el puntero localmente (no toca el backend)."""
        self._alive = False


# Helper para construir el puntero
def make_pointer(
    storage: StorageService,
    ref: BallRef,
    *,
    consume: bool = False,
    delete_remote: bool = False,
) -> AxoPointer:
    return AxoPointer(storage, ref, consume=consume, delete_remote=delete_remote)

