"""
axo/core/axo.py
~~~~~~~~~~~~~~~

Core building blocks for the *Axo* programming model:

*   **Decorators**

    * :func:`axo_method` – transparently routes a *synchronous* method call
      through the active‑object runtime (local or distributed).
    * :func:`axo_task`   – schedules a long‑running or asynchronous task.

*   **Pydantic metadata model** – :class:`MetadataX`.

*   **Base active‑object class** – :class:`Axo`.

The module relies on `option` (``Ok`` / ``Err`` / ``Result``) for explicit
error handling and uses *cloudpickle* to (de)serialise user‑defined classes.
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────── stdlib ──
import inspect
import logging
import os
import re
import struct
import string
import time as _t
import types
from abc import ABC
from functools import wraps
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Annotated
)

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
import cloudpickle as cp
from nanoid import generate as nanoid
from option import Err, Ok, Result
from pydantic import BaseModel, Field
from pydantic.functional_validators import AfterValidator

# ──────────────────────────────────────────────────────────────── project ───
from axo.runtime import get_runtime
from axo.utils import serialize_and_yield_chunks
from mictlanx.logger.log import Log

# ───────────────────────────────────────────────────────────────── constants ─
ALPHABET = string.ascii_lowercase + string.digits
AXO_ID_SIZE = int(os.getenv("AXO_ID_SIZE", "16"))
AXO_DEBUG = bool(int(os.getenv("AXO_DEBUG", "1")))
AXO_PRODUCTION_LOG_ENABLE = bool(int(os.getenv("AXO_PRODUCTION_LOG_ENABLE", "0")))
AXO_LOG_PATH = os.getenv("AXO_LOG_PATH", "/axo/log")
AXO_PROPERTY_PREFIX = "_acx_property_"

R = TypeVar("R")  # generic return type for Axo.call

# ─────────────────────────────────────────────────────── logger configuration
if AXO_PRODUCTION_LOG_ENABLE:
    logger = Log(
        name=__name__,
        console_handler_filter=lambda _: AXO_DEBUG,
        path=AXO_LOG_PATH,
    )
else:
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        _h = logging.StreamHandler()
        _h.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(_h)
    logger.setLevel(logging.DEBUG)


# =========================================================================== #
# Small helpers
# =========================================================================== #
def _generate_id(val: str | None, *, size: int = AXO_ID_SIZE) -> str:
    """Return a valid identifier containing only a‑z 0‑9 _ ."""
    if not val:
        return nanoid(alphabet=ALPHABET, size=size)
    return re.sub(r"[^a-z0-9_]", "", val)


def _make_id_validator(size: int) -> Callable[[str | None], str]:
    """Factory that produces a pydantic *AfterValidator* for ID fields."""

    def _validator(v: str | None) -> str:
        return _generate_id(v, size=size)

    return _validator


# pydantic *Annotated* alias for object keys
AxoObjectId = Annotated[Optional[str], AfterValidator(_make_id_validator(AXO_ID_SIZE))]


# =========================================================================== #
# Decorators
# =========================================================================== #
def axo_method(func: Callable[..., R]) -> Callable[..., Result[R, Exception]]:
    """
    Decorator that routes a *method call* through the runtime.

    *   Collects bucket/key information and injects them into **kwargs**.
    *   If the runtime is distributed and the object is still *local*,
        it is first *persistified*.
    *   Returns a :class:`Result`.
    """

    @wraps(func)
    def _wrapper(self: "Axo", *args: Any, **kwargs: Any) -> Result[R, Exception]:
        try:
            start = _t.time()
            rt = get_runtime()  # current LocalRuntime or DistributedRuntime
            ep = rt.endpoint_manager.get_endpoint(kwargs.get("endpoint_id", ""))
            self.set_endpoint_id(ep.endpoint_id)

            # Inject default kwargs if missing
            kwargs.setdefault("endpoint_id", ep.endpoint_id)
            kwargs.setdefault("axo_key", self.get_axo_key())
            kwargs.setdefault("axo_bucket_id", self.get_axo_bucket_id())
            kwargs.setdefault("sink_bucket_id", self.get_sink_bucket_id())
            kwargs.setdefault("source_bucket_id", self.get_source_bucket_id())
            if not rt.is_distributed:
                kwargs.setdefault("storage", rt.storage_service)

            # Persist local instance if we are about to call a remote endpoint
            if rt.is_distributed and self._acx_local:
                self.persistify()

            logger.debug(
                {
                    "event": "METHOD.EXEC",
                    "fname": func.__name__,
                    **{k: kwargs[k] for k in ("axo_key", "endpoint_id")},
                }
            )
            res = ep.method_execution(
                key=self.get_axo_key(),
                fname=func.__name__,
                ao=self,
                f=func,
                fargs=args,
                fkwargs=kwargs,
            )
            logger.info(
                {
                    "event": "METHOD.EXEC",
                    "fname": func.__name__,
                    "response_time": _t.time() - start,
                }
            )
            return res
        except Exception as exc:
            logger.exception("METHOD.EXEC failed")
            return Err(exc)

    _wrapper.original = func  # type: ignore[attr-defined]
    return _wrapper


def axo_task(func: Callable[..., R]) -> Callable[..., Result[bool, Exception]]:
    """
    Decorator that *enqueues* a task to be executed by the runtime
    instead of running synchronously.
    """

    @wraps(func)
    def _wrapper(self: "Axo", *args: Any, **kwargs: Any) -> Result[bool, Exception]:
        try:
            rt = get_runtime()
            ep = rt.endpoint_manager.get_endpoint(kwargs.get("endpoint_id", ""))
            self.set_endpoint_id(ep.endpoint_id)

            # Persist object first if needed
            if rt.is_distributed and self._acx_local:
                self.persistify().unwrap()  # raise on failure

            payload = {
                "fname": func.__name__,
                "axo_key": self.get_axo_key(),
                "axo_bucket_id": self.get_axo_bucket_id(),
                "sink_bucket_id": self.get_sink_bucket_id(),
                "source_bucket_id": self.get_source_bucket_id(),
            }
            ep.task_execution(task_function=func, payload=payload)
            return Ok(True)
        except Exception as exc:
            logger.exception("TASK.EXEC failed")
            return Err(exc)

    _wrapper.original = func  # type: ignore[attr-defined]
    return _wrapper


# =========================================================================== #
# Metadata (pydantic model)
# =========================================================================== #
class MetadataX(BaseModel):
    """Serializable metadata stored alongside every active object."""

    # Class‑level defaults (paths can be overridden via env‑vars)
    path: ClassVar[str] = os.getenv("ACTIVE_LOCAL_PATH", "/axo/data")
    source_path: ClassVar[str] = os.getenv("AXO_SOURCE_PATH", "/axo/source")
    sink_path: ClassVar[str] = os.getenv("AXO_SINK_PATH", "/axo/sink")

    # Stored fields
    pivot_storage_node: Optional[str] = ""
    is_read_only: bool = False

    axo_key: AxoObjectId = ""
    module: str
    name: str
    class_name: str
    version: str = "v0"

    axo_bucket_id: str = ""
    source_bucket_id: str = ""
    sink_bucket_id: str = ""

    endpoint_id: str = ""
    dependencies: List[str] = Field(default_factory=list)

    # ------------------------------------------------------------------ #
    # pydantic hook
    # ------------------------------------------------------------------ #
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Fill IDs if empty
        self.axo_key = _generate_id(self.axo_key)
        self.axo_bucket_id = _generate_id(self.axo_bucket_id, size=AXO_ID_SIZE * 2)
        self.sink_bucket_id = _generate_id(self.sink_bucket_id, size=AXO_ID_SIZE * 2)
        self.source_bucket_id = _generate_id(
            self.source_bucket_id, size=AXO_ID_SIZE * 2
        )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def to_json_with_string_values(self) -> Dict[str, str]:
        """Return ``dict`` with every value stringified (for tags)."""
        out: Dict[str, str] = {}
        for k, v in self.model_dump().items():
            out[k] = ";".join(v) if isinstance(v, list) else str(v)
        return out


# =========================================================================== #
# Base active‑object class
# =========================================================================== #
class AxoMeta(type):
    def __call__(cls, *args, **kwargs):
        # mutate / inject anything you want

        # create the instance and run __init__
        return super().__call__(*args, **kwargs)

class Axo(metaclass=AxoMeta):
    """
    Root class for all active objects.

    Sub‑classes automatically receive an :class:`MetadataX` instance and a
    plethora of helper methods (persist, call, etc.).
    """

    # Runtime/caching flags (filled by decorators and runtime)
    _acx_metadata: MetadataX
    _acx_local: bool = True
    _acx_remote: bool = False

    # ------------------------------------------------------------------ #
    # Fast dynamic invoker
    # ------------------------------------------------------------------ #
    @staticmethod
    def call(
        instance: "Axo", method_name: str, *args: Any, **kwargs: Any
    ) -> Result[R, Exception]:
        """
        Dynamically invoke *method_name* on *instance*.

        Returns ``Ok(result)`` or ``Err(exception)``.
        """
        try:
            attr = getattr(instance, method_name)
            if callable(attr):
                return Ok(attr(*args, **kwargs))
            return Ok(attr)
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # Bucket & endpoint helpers
    # ------------------------------------------------------------------ #
    def get_axo_key(self) -> str: 
        """Return the unique object key."""
        return self._acx_metadata.axo_key

    def get_axo_bucket_id(self) -> str:
        return self._acx_metadata.axo_bucket_id

    # -- sink -----------------------------------------------------------
    def set_sink_bucket_id(self, sink_bucket_id: str = "") -> str:
        self._acx_metadata.sink_bucket_id = _generate_id(
            sink_bucket_id, size=AXO_ID_SIZE * 2
        )
        return self._acx_metadata.sink_bucket_id

    def get_sink_bucket_id(self) -> str:
        return self._acx_metadata.sink_bucket_id or self.set_sink_bucket_id()

    # -- source ---------------------------------------------------------
    def set_source_bucket_id(self, source_bucket_id: str = "") -> str:
        self._acx_metadata.source_bucket_id = _generate_id(
            source_bucket_id, size=AXO_ID_SIZE * 2
        )
        return self._acx_metadata.source_bucket_id

    def get_source_bucket_id(self) -> str:
        return self._acx_metadata.source_bucket_id or self.set_source_bucket_id()

    # -- endpoint -------------------------------------------------------
    def set_endpoint_id(self, endpoint_id: str = "") -> str:
        eid = (
            f"axo-endpoint-{_generate_id('', size=8)}" if not endpoint_id else endpoint_id
        )
        self._acx_metadata.endpoint_id = eid
        return eid

    def get_endpoint_id(self) -> str:
        return self._acx_metadata.endpoint_id or self.set_endpoint_id()

    # ------------------------------------------------------------------ #
    # Construction magic
    # ------------------------------------------------------------------ #
    def __new__(cls, *args: Any, **kwargs: Any) -> "Axo":
        # print(kwargs)
        obj = super().__new__(cls)
        obj._acx_metadata = MetadataX(
            class_name=cls.__name__, module=cls.__module__, name=cls.__name__
        )
        obj._acx_metadata.axo_bucket_id = kwargs.get("axo_bucket_id",obj._acx_metadata.axo_bucket_id)
        obj._acx_metadata.source_bucket_id = kwargs.get("source_bucket_id",obj._acx_metadata.source_bucket_id)
        obj._acx_metadata.sink_bucket_id = kwargs.get("sink_bucket_id",obj._acx_metadata.sink_bucket_id)
        obj._acx_metadata.axo_key = kwargs.get("axo_key",obj._acx_metadata.axo_key)
        obj._acx_metadata.endpoint_id = kwargs.get("endpoint_id",obj._acx_metadata.endpoint_id)
        return obj
    
    def __init__(self,*args,**kwargs):
        pass

    # ------------------------------------------------------------------ #
    # Serialisation helpers
    # ------------------------------------------------------------------ #
    def to_bytes(self) -> bytes:
        """
        Serialise *attributes*, *methods*, *class definition* and
        *class source code* into a single byte‑buffer:

            [len][attrs]  [len][methods]  [len][class_def]  [len][src]
        """
        attrs_b = cp.dumps(self.__dict__)
        methods_b = cp.dumps(
            {
                n: getattr(self.__class__, n) 
                for n in dir(self.__class__) 
                if callable(getattr(self.__class__, n))
            }
        )
        class_def_b = cp.dumps(self.__class__)
        src_b = cp.dumps(inspect.getsource(self.__class__).encode())

        out = b""
        for part in (attrs_b, methods_b, class_def_b, src_b):
            out += struct.pack("I", len(part)) + part
        return out

    def to_stream(self,chunk_size: str = "1MB") -> Generator[bytes, None, None]:
        """Yield ``self`` as (roughly) *chunk_size* byte blocks."""
        return serialize_and_yield_chunks(self, chunk_size=chunk_size)

    # -- Deserialisation -----------------------------------------------
    @staticmethod
    def from_bytes(raw: bytes, include_original: bool = False) -> Result["Axo", Exception]:
        """
        Re‑create an :class:`Axo` instance from :pydata:`raw`.

        If *include_original* is True and a method was decorated with
        :func:`axo_method` / :func:`axo_task` the undecorated function is bound.
        """
        try:
            parts: list[Any] = []
            idx = 0
            while idx < len(raw):
                length = struct.unpack_from("I", raw, idx)[0]
                idx += 4
                parts.append(cp.loads(raw[idx : idx + length]))
                idx += length

            attrs, methods, class_def = parts[:3]
            obj:Axo = class_def.__new__(class_def)
            # Restore state & methods
            skip ={"__class__", "__dict__", "__module__", "__weakref__"} 
            for k, v in attrs.items():
                if k not in skip:
                    setattr(obj, k, v)
            for name, fn in methods.items():
                if name in skip:
                    continue  
                fn = fn.original if include_original and hasattr(fn, "original") else fn
                setattr(obj, name, types.MethodType(fn, obj))

            return Ok(obj)
        except Exception as exc:
            return Err(exc)

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #
    def persistify(self, *, bucket_id: str = "", key: str = "") -> Result[str, Exception]:
        """
        Store the current instance (bytes + class def) using the active runtime.

        Returns the *key* used to store the object.
        """
        try:
            rt = get_runtime()
            key = key or self.get_axo_key()
            bucket_id = bucket_id or self.get_axo_bucket_id()
            if rt is None:
                raise Exception("No runtime was initialized.")
            # print(rt)
            raise Exception("BOOM!")
            # Persist via runtime helper
            res = rt.persistify(instance=self, bucket_id=bucket_id, key=key)
            self._acx_remote = res.is_ok
            self._acx_local = not res.is_ok
            return res
        except Exception as exc:
            return Err(exc)

    # Convenience: fetch by key/bucket
    @staticmethod
    def get_by_key(key: str, *, bucket_id: str = "") -> Result["Axo", Exception]:
        return get_runtime().get_active_object(key=key, bucket_id=bucket_id)
