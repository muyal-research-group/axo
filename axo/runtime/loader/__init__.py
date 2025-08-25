from __future__ import annotations
import json
import types
from typing import Any, Dict, Optional, Type
# 
from option import Ok, Err, Result
# 
from axo.storage import AxoStorage
from axo.storage.types import AxoStorageMetadata
from axo.errors import AxoError, AxoErrorType
from axo.log import get_logger

logger = get_logger(__name__)


class AxoLoader:
    """
    Loads an Axo object from AxoStorage:
      1) fetch blobs (source_code + attrs) and their metadata
      2) resolve class name (from tag or parameter)
      3) exec source in a controlled module namespace
      4) instantiate class with attrs (dict)

    This layer is Axo-aware but *storage-agnostic* (uses AxoStorage only).
    """

    def __init__(
        self,
        storage: AxoStorage,
        *,
        api_globals: Optional[Dict[str, Any]] = None,
        safe_builtins: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        api_globals: injected symbols visible to user code (e.g., Axo base class, decorators)
        safe_builtins: optionally restrict builtins for exec (pass {} for very restrictive)
        """
        self.storage = storage
        self.api_globals = dict(api_globals or {})
        self.safe_builtins = dict(safe_builtins or {})

    # --------------------------- public API ---------------------------

    async def load_object(
        self,
        *,
        bucket_id: str,
        key: str,
        class_name: Optional[str] = None,
    ) -> Result[Any, AxoError]:
        """
        High-level: returns a fully constructed instance of the class stored under `key`.
        """
        blobs_res = await self.storage.get_blobs(bucket_id=bucket_id, key=key)
        if blobs_res.is_err:
            return Err(blobs_res.unwrap_err())

        blobs = blobs_res.unwrap()
        src = blobs.source_code_blob
        at  = blobs.attrs_blob
        print("SEC_MET", src.metadata)
        # 1) figure out class name
        cls_name = (
            class_name
            or self._class_name_from_tags(src.metadata)
            or self._class_name_from_tags(at.metadata)
        )
        print("CLASS_+NAME", class_name)


        if not cls_name:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, "missing axo_class_name tag"))

        # 2) decode attrs
        attrs_res = self._decode_attrs(at.data)
        if attrs_res.is_err:
            return Err(attrs_res.unwrap_err())
        attrs = attrs_res.unwrap()

        # 3) exec + get class
        load_cls_res = self.__exec_source_and_resolve_class(src.data, cls_name)
        if load_cls_res.is_err:
            return Err(load_cls_res.unwrap_err())
        Cls = load_cls_res.unwrap()

        # 4) construct
        try:
            instance = Cls(**attrs) if isinstance(attrs, dict) else Cls(attrs)
            return Ok(instance)
        except TypeError as e:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, f"__init__ mismatch: {e}"))
        except Exception as e:
            return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, f"constructor failed: {e}"))

    async def load_class(
        self, *, bucket_id: str, key: str, class_name: Optional[str] = None
    ) -> Result[Type[Any], AxoError]:
        """
        Lower-level: returns the Python class type defined in the stored source.
        """
        blobs_res = await self.storage.get_blobs(bucket_id=bucket_id, key=key)
        if blobs_res.is_err:
            return Err(blobs_res.unwrap_err())

        src_md = blobs_res.unwrap().source_code_blob.metadata
        cls_name = class_name or self._class_name_from_tags(src_md)
        if not cls_name:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, "missing axo_class_name tag"))

        return self.__exec_source_and_resolve_class(
            blobs_res.unwrap().source_code_blob.data, cls_name
        )

    # -------------------------- helpers --------------------------

    @staticmethod
    def _class_name_from_tags(md: AxoStorageMetadata | None) -> Optional[str]:
        try:
            return (md.tags or {}).get("axo_class_name")
        except Exception:
            return None

    def _decode_attrs(self, raw: bytes) -> Result[Dict[str, Any] | Any, AxoError]:
        """
        Try JSON first; fallback to cloudpickle; else error.
        """
        # JSON
        try:
            return Ok(json.loads(raw.decode("utf-8")))
        except Exception:
            pass

        # cloudpickle fallback (optional dependency)
        try:
            import cloudpickle as cp  # local import to avoid hard dep
            return Ok(cp.loads(raw))
        except Exception as e:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, f"attrs decode failed: {e}"))

    def __exec_source_and_resolve_class(
        self, src_bytes: bytes, class_name: str
    ) -> Result[Type[Any], AxoError]:
        """
        Compile + exec in an isolated module dict; then fetch class by name.
        """
        try:
            src = src_bytes.decode("utf-8")
        except Exception as e:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, f"source not utf-8: {e}"))

        mod = types.ModuleType("__axo_dynamic__")
        # build globals for exec
        g: Dict[str, Any] = {"__name__": mod.__name__, "__builtins__": self.safe_builtins or __builtins__}
        g.update(self.api_globals)
        try:
            exec(compile(src, filename=f"<axo:{class_name}>", mode="exec"), g, g)
        except SyntaxError as e:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, f"syntax error: {e}"))
        except Exception as e:
            return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, f"exec failed: {e}"))

        try:
            Cls = g[class_name]
        except KeyError:
            return Err(AxoError.make(AxoErrorType.NOT_FOUND, f"class {class_name} not defined"))
        if not isinstance(Cls, type):
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, f"{class_name} is not a class"))
        return Ok(Cls)
