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
import sys
import os
import re
import struct
import string
import time as T
import types
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
    Annotated,
    cast
)

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
import cloudpickle as cp
from nanoid import generate as nanoid
from option import Err, Ok, Result
from pydantic import BaseModel, Field
from pydantic.functional_validators import AfterValidator
import wrapt

# ──────────────────────────────────────────────────────────────── project ───
from axo.runtime import get_runtime
from axo.utils import serialize_and_yield_chunks
from axo.log import get_logger
# from axo.endpoint.endpoint import EndpointX

# ───────────────────────────────────────────────────────────────── constants ─
ALPHABET = string.ascii_lowercase + string.digits
AXO_ID_SIZE = int(os.getenv("AXO_ID_SIZE", "16"))
AXO_DEBUG = bool(int(os.getenv("AXO_DEBUG", "1")))
AXO_PRODUCTION_LOG_ENABLE = bool(int(os.getenv("AXO_PRODUCTION_LOG_ENABLE", "0")))
AXO_LOG_PATH = os.getenv("AXO_LOG_PATH", "/axo/log")
AXO_PROPERTY_PREFIX = "_acx_property_"

R = TypeVar("R")  # generic return type for Axo.call

# ─────────────────────────────────────────────────────── logger configuration
logger = get_logger(name=__name__)

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
# @wrapt.decorator

def axo_method(wrapped: Callable[..., R]) -> Callable[..., Result[R, Exception]]:
    @wrapt.decorator
    def _wrapper(wrapped_func, instance, args, kwargs):
        try:
            t1 = T.time()
            rt = get_runtime()
            ep = rt.endpoint_manager.get_endpoint(kwargs.get("axo_endpoint_id", "") )
            instance.set_endpoint_id(ep.endpoint_id)

            kwargs.setdefault("axo_endpoint_id", ep.endpoint_id)
            kwargs.setdefault("axo_key", instance.get_axo_key())
            kwargs.setdefault("axo_bucket_id", instance.get_axo_bucket_id())
            kwargs.setdefault("axo_sink_bucket_id", instance.get_sink_bucket_id())
            kwargs.setdefault("axo_source_bucket_id", instance.get_source_bucket_id())
            # is_distributed =  rt.get_is_distributed
            if not rt.is_distributed:
                kwargs.setdefault("storage", rt.storage_service)
            
            if rt.is_distributed and instance._acx_local:
                return Err(Exception("First you must persistify the object."))
            fname = wrapped_func.__name__
            res = ep.method_execution(
                key     = instance.get_axo_key(),
                fname   = fname,
                ao      = instance,
                f       = wrapped_func,
                fargs   = args,
                fkwargs = kwargs,
            )
            logger.info({
                "event": "METHOD.EXEC",
                "mode":"DISTRIBUTED" if rt.is_distributed else "LOCAL",
                "fname": fname,
                "ok": res.is_ok,
                "response_time": T.time() - t1,  # update this properly
            })
            return res
        except Exception as e:
            logger.error(f"METHOD.EXEC failed: {e}")
            return Err(e)

    return cast(Callable[..., Result[R, Exception]],_wrapper(wrapped))


# def axo_method(func: Callable[..., R]) -> Callable[..., Result[R, Exception]]:
#     """
#     Decorator that routes a *method call* through the runtime.

#     *   Collects bucket/key information and injects them into **kwargs**.
#     *   If the runtime is distributed and the object is still *local*,
#         it is first *persistified*.
#     *   Returns a :class:`Result`.
#     """

#     @wraps(func)
#     def _wrapper(self: "Axo", *args: Any, **kwargs: Any) -> Result[R, Exception]:
#         try:
#             start = T.time()
#             rt = get_runtime()  # current LocalRuntime or DistributedRuntime
#             ep = rt.endpoint_manager.get_endpoint(kwargs.get("axo_endpoint_id", "") )
#             self.set_endpoint_id(ep.endpoint_id)

#             # Inject default kwargs if missing
#             kwargs.setdefault("axo_endpoint_id", ep.endpoint_id)
#             kwargs.setdefault("axo_key", self.get_axo_key())
#             kwargs.setdefault("axo_bucket_id", self.get_axo_bucket_id())
#             kwargs.setdefault("axo_sink_bucket_id", self.get_sink_bucket_id())
#             kwargs.setdefault("axo_source_bucket_id", self.get_source_bucket_id())
#             if not rt.get_is_distributed:
#                 kwargs.setdefault("storage", rt.storage_service)

#             # Persist local instance if we are about to call a remote endpoint
#             # print(self._acx_local, rt.is_distributed)
#             if rt.is_distributed and self._acx_local:
#                 return Err(Exception("First you must persistify the object."))
#                 # self.persistify()

#             res = ep.method_execution(
#                 key=self.get_axo_key(),
#                 fname=func.__name__,
#                 ao=self,
#                 f=func,
#                 fargs=args,
#                 fkwargs=kwargs,
#             )
#             logger.info(
#                 {
#                     "event": "METHOD.EXEC",
#                     "fname": func.__name__,
#                     "ok":res.is_ok,
#                     "response_time": T.time() - start,
#                 }
#             )
#             return res
#         except Exception as e:
#             logger.error(f"METHOD.EXEC failed: {e}")
#             return Err(e)

#     _wrapper.original = func  # type: ignore[attr-defined]
#     return _wrapper


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
    axo_pivot_storage_node: Optional[str] = ""
    axo_is_read_only: bool = False

    axo_key: AxoObjectId = ""
    axo_module: str
    axo_name: str
    axo_class_name: str
    axo_version: str = "v0"

    axo_bucket_id: str = ""
    axo_source_bucket_id: str = ""
    axo_sink_bucket_id: str = ""

    axo_endpoint_id: str = ""
    axo_dependencies: List[str] = Field(default_factory=list)

    # ------------------------------------------------------------------ #
    # pydantic hook
    # ------------------------------------------------------------------ #
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Fill IDs if empty
        self.axo_key = _generate_id(self.axo_key)
        self.axo_bucket_id = _generate_id(self.axo_bucket_id, size=AXO_ID_SIZE * 2)
        self.axo_sink_bucket_id = _generate_id(self.axo_sink_bucket_id, size=AXO_ID_SIZE * 2)
        self.axo_source_bucket_id = _generate_id(
            self.axo_source_bucket_id, size=AXO_ID_SIZE * 2
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
        self._acx_metadata.axo_sink_bucket_id = _generate_id(
            sink_bucket_id, size=AXO_ID_SIZE * 2
        )
        return self._acx_metadata.axo_sink_bucket_id

    def get_sink_bucket_id(self) -> str:
        return self._acx_metadata.axo_sink_bucket_id or self.set_sink_bucket_id()

    # -- source ---------------------------------------------------------
    def set_source_bucket_id(self, source_bucket_id: str = "") -> str:
        self._acx_metadata.axo_source_bucket_id = _generate_id(
            source_bucket_id, size=AXO_ID_SIZE * 2
        )
        return self._acx_metadata.axo_source_bucket_id

    def get_source_bucket_id(self) -> str:
        return self._acx_metadata.axo_source_bucket_id or self.set_source_bucket_id()

    # -- endpoint -------------------------------------------------------
    def set_endpoint_id(self, endpoint_id: str = "") -> str:
        eid = (
            f"axo-endpoint-{_generate_id('', size=8)}" if not endpoint_id else endpoint_id
        )
        self._acx_metadata.axo_endpoint_id = eid
        return eid

    def get_endpoint_id(self) -> str:
        # print()
        e_id = self._acx_metadata.axo_endpoint_id
        return self.set_endpoint_id() if e_id == None else ""
        # return self._acx_metadata.axo_endpoint_id  or self.set_endpoint_id()

    # ------------------------------------------------------------------ #
    # Construction magic
    # ------------------------------------------------------------------ #
    def __new__(cls, *args: Any, **kwargs: Any) -> "Axo":
        # print(kwargs)
        obj = super().__new__(cls)
        obj._acx_metadata = MetadataX(
            axo_class_name=cls.__name__, axo_module=cls.__module__, axo_name=cls.__name__
        )
        obj._acx_metadata.axo_bucket_id = kwargs.get("axo_bucket_id",obj._acx_metadata.axo_bucket_id)
        obj._acx_metadata.axo_source_bucket_id = kwargs.get("axo_source_bucket_id",obj._acx_metadata.axo_source_bucket_id)
        obj._acx_metadata.axo_sink_bucket_id = kwargs.get("axo_sink_bucket_id",obj._acx_metadata.axo_sink_bucket_id)
        obj._acx_metadata.axo_key = kwargs.get("axo_key",obj._acx_metadata.axo_key)
        obj._acx_metadata.axo_endpoint_id = kwargs.get("axo_endpoint_id",obj._acx_metadata.axo_endpoint_id)
        return obj
    
    def __init__(self,*args,**kwargs):
        pass

    # ------------------------------------------------------------------ #
    # Serialisation helpers
    # ------------------------------------------------------------------ #
    def to_bytes(self) -> bytes:
        """
        Serialise *attributes*, and
        *class source code* into a single byte‑buffer:

            [len][attrs]  [len][src]
        """
        attrs = cp.dumps(self.__dict__)
        # methods = cp.dumps({
        #     k: getattr(self, k) for k in dir(self)
        #     if callable(getattr(self, k)) and not k.startswith("__")
        # })
        # class_def = cp.dumps(self.__class__)
        class_code = inspect.getsource(self.__class__).encode("utf-8")  # ✅ ENCODED

        parts = [attrs,  class_code]
        packed = b""
        for part in parts:
            packed += struct.pack("I", len(part)) + part
        return packed
    
    def  get_raw_parts(self)->Tuple[Dict[str, Any], Dict[str, Any], Type[Axo], str]:
        attrs = self.__dict__
        print(attrs)
        methods = {
            k: getattr(self, k) for k in dir(self)
            if callable(getattr(self, k)) and not k.startswith("__")
        }
        class_def = self.__class__
        class_code = inspect.getsource(self.__class__)
        # .encode("utf-8")  # ✅ ENCODED

        return attrs, methods, class_def, class_code
        # attrs_b = cp.dumps(self.__dict__)
        # methods_b = cp.dumps(
        #     {
        #         n: getattr(self.__class__, n) 
        #         for n in dir(self.__class__) 
        #         if callable(getattr(self.__class__, n))
        #     }
        # )
        # class_def_b = cp.dumps(self.__class__)
        # src_b = cp.dumps(inspect.getsource(self.__class__).encode())

        # out = b""
        # for part in (attrs_b, methods_b, class_def_b, src_b):
        #     out += struct.pack("I", len(part)) + part
        # return out

    def to_stream(self,chunk_size: str = "1MB") -> Generator[bytes, None, None]:
        """Yield ``self`` as (roughly) *chunk_size* byte blocks."""
        return serialize_and_yield_chunks(self, chunk_size=chunk_size)

    # @staticmethod
    # def get_parts(raw_obj: bytes) -> Result[Tuple[Dict[str, Any], Dict[str, Any], Type[Axo], str], Exception]:
    #     try:
    #         index = 0
    #         unpacked_data = []

    #         # First, read lengths and raw segments
    #         raw_parts = []
    #         while index < len(raw_obj):
    #             length = struct.unpack_from('I', raw_obj, index)[0]
    #             index += 4
    #             data = raw_obj[index:index+length]
    #             index += length
    #             raw_parts.append(data)

    #         # The last part is the source code of the class (in bytes)
    #         class_code_bytes = raw_parts[-1]
    #         code_str = class_code_bytes.decode("utf-8")

    #         # Dynamically evaluate the class code into a temporary module
    #         module_name = "__axo_dynamic__"
    #         mod = types.ModuleType(module_name)
    #         sys.modules[module_name] = mod
    #         exec(code_str, mod.__dict__)

    #         # Find the class that inherits from Axo
    #         rebuilt_class = None
    #         for obj in mod.__dict__.values():
    #             if isinstance(obj, type) and issubclass(obj, Axo) and obj.__name__ != "Axo":
    #                 rebuilt_class = obj
    #                 break
    #         if rebuilt_class is None:
    #             raise Exception("No valid Axo class could be rebuilt")

    #         # Now safely deserialize the rest
    #         attrs = cp.loads(raw_parts[0])
    #         methods = cp.loads(raw_parts[1])
    #         class_df = rebuilt_class  # You can also `cp.loads(raw_parts[2])` if needed

    #         return Ok((attrs, methods, class_df, code_str))

    #     except Exception as e:
    #         return Err(e)
        
    @staticmethod
    def get_parts(raw_obj: bytes) -> Result[Tuple[Dict[str, Any], Dict[str, Any], Type[Axo], str], Exception]:
        try:
            index = 0
            unpacked_data = []

            # First, read lengths and raw segments
            raw_parts = []
            while index < len(raw_obj):
                length = struct.unpack_from('I', raw_obj, index)[0]
                index += 4
                data = raw_obj[index:index+length]
                index += length
                raw_parts.append(data)

            # The last part is the source code of the class (in bytes)
            class_code_bytes = raw_parts[-1]
            code_str = class_code_bytes.decode("utf-8")

            # Dynamically evaluate the class code into a temporary module
            module_name = "axo.dynamic"
            mod = types.ModuleType(module_name)
            mod.__dict__["Axo"] = Axo
            mod.__dict__["axo_method"] = axo_method
            sys.modules[module_name] = mod

            exec(code_str, mod.__dict__)

            # Find the class that inherits from Axo
            rebuilt_class = None
            for obj in mod.__dict__.values():
                if isinstance(obj, type) and issubclass(obj, Axo) and obj.__name__ != "Axo":
                    rebuilt_class = obj
                    break
            if rebuilt_class is None:
                raise Exception("No valid Axo class could be rebuilt")
            rebuilt_class.__module__ = module_name
            
            # Now safely deserialize the rest
            attrs = cp.loads(raw_parts[0])
            methods = cp.loads(raw_parts[1])
            class_df = rebuilt_class  # You can also `cp.loads(raw_parts[2])` if needed

            return Ok((attrs, methods, class_df, code_str))

        except Exception as e:
            return Err(e)
    # -- Deserialisation -----------------------------------------------
    @staticmethod
    def from_bytes(raw: bytes, include_original: bool = False) -> Result["Axo", Exception]:
        """
        Re‑create an :class:`Axo` instance from :pydata:`raw`.

        If *include_original* is True and a method was decorated with
        :func:`axo_method` / :func:`axo_task` the undecorated function is bound.
        """
        try:
            parts = []
            idx = 0
            while idx < len(raw):
                length = struct.unpack_from("I", raw, idx)[0]
                idx += 4
                parts.append(raw[idx:idx+length])
                idx += length

            attrs = cp.loads(parts[0])
            # methods = cp.loads(parts[1])
            class_code_str = parts[1].decode("utf-8")
            print("CLASS", class_code_str)
            # .decode("utf-8")

            # Dynamically execute the class definition

            module_name = "__axo_dynamic__"
            mod = types.ModuleType(module_name)

            # ✅ Inject 'Axo' base class into the module's namespace
            mod.__dict__["Axo"] = Axo
            mod.__dict__["axo_method"] = axo_method
            sys.modules[module_name] = mod

            # Now execute the class code with the proper Axo definition
            exec(class_code_str, mod.__dict__)

            # Find the subclass of Axo
            rebuilt_class = None
            for obj in mod.__dict__.values():
                if isinstance(obj, type) and issubclass(obj, Axo) and obj.__name__ != "Axo":
                    rebuilt_class = obj
                    break

            # if rebuilt_class is None:
            #     return Err(Exception("No valid Axo subclass found in class definition"))

            obj: Axo = rebuilt_class.__new__(rebuilt_class)
            skip = {"__class__", "__dict__", "__module__", "__weakref__"}
            for k, v in attrs.items():
                if k not in skip:
                    setattr(obj, k, v)
            # for name, fn in methods.items():
            #     if name not in skip:
            #         fn = fn.original if include_original and hasattr(fn, "original") else fn
            #         setattr(obj, name, types.MethodType(fn, obj))

            return Ok(obj)

        except Exception as e:
            return Err(e)       
        # try:
        #     parts: list[Any] = []
        #     idx = 0
        #     while idx < len(raw):
        #         length = struct.unpack_from("I", raw, idx)[0]
        #         idx += 4
        #         parts.append(cp.loads(raw[idx : idx + length]))
        #         idx += length

        #     attrs, methods, class_def = parts[:3]
        #     obj:Axo = class_def.__new__(class_def)
        #     # Restore state & methods
        #     skip ={"__class__", "__dict__", "__module__", "__weakref__"} 
        #     for k, v in attrs.items():
        #         if k not in skip:
        #             setattr(obj, k, v)
        #     for name, fn in methods.items():
        #         if name in skip:
        #             continue  
        #         fn = fn.original if include_original and hasattr(fn, "original") else fn
        #         setattr(obj, name, types.MethodType(fn, obj))

        #     return Ok(obj)
        # except Exception as exc:
        #     return Err(exc)

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #
    async def persistify(self, *, bucket_id: str = "", key: str = "") -> Result[str, Exception]:
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
            # Persist via runtime helper
            res = await rt.persistify(instance=self, bucket_id=bucket_id, key=key)
            self._acx_remote = res.is_ok
            self._acx_local = not res.is_ok
            return res
        except Exception as exc:
            return Err(exc)

    # Convenience: fetch by key/bucket
    @staticmethod
    async def get_by_key(key: str, *, bucket_id: str = "") -> Result["Axo", Exception]:
        return await get_runtime().get_active_object(key=key, bucket_id=bucket_id)
