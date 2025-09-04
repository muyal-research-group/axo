
from pydantic import BaseModel,Field,field_validator,ConfigDict,model_validator
from typing import ClassVar,Optional,List,Dict,Any,Tuple
import os
import re
# 
from axo.helpers import _generate_id,_build_axo_uri

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