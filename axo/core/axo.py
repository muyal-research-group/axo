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
import struct
import time as T
import types
from typing import (
    Any,
    Dict,
    Generator,
    Tuple,
    Type,
    TypeVar,
)

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
import cloudpickle as cp
from option import Err, Ok, Result
import humanfriendly as HF
# ──────────────────────────────────────────────────────────────── project ───
from axo.utils import serialize_and_yield_chunks
from axo.log import get_logger
from axo.core.decorators import axo_method
from axo.runtime import get_runtime
from axo.core.models import MetadataX
from axo.helpers import _generate_id
from axo.environment import AXO_ID_SIZE
# ───────────────────────────────────────────────────────────────── constants ─
AXO_DEBUG = bool(int(os.getenv("AXO_DEBUG", "1")))
AXO_PRODUCTION_LOG_ENABLE = bool(int(os.getenv("AXO_PRODUCTION_LOG_ENABLE", "0")))
AXO_LOG_PATH = os.getenv("AXO_LOG_PATH", "/axo/log")
AXO_PROPERTY_PREFIX = "_acx_property_"


# ─────────────────────────────────────────────────────── logger configuration
logger = get_logger(name=__name__)

R = TypeVar("R")  # generic return type for Axo.call
# =========================================================================== #
# Small helpers
# =========================================================================== #





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
                return attr(*args, **kwargs)
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
            sink_bucket_id
        )
        return self._acx_metadata.axo_sink_bucket_id

    def get_axo_sink_bucket_id(self) -> str:
        return self._acx_metadata.axo_sink_bucket_id or self.set_sink_bucket_id()

    # -- source ---------------------------------------------------------
    def set_source_bucket_id(self, source_bucket_id: str = "") -> str:
        self._acx_metadata.axo_source_bucket_id = _generate_id(
            source_bucket_id, size=AXO_ID_SIZE
        )
        return self._acx_metadata.axo_source_bucket_id

    def get_axo_source_bucket_id(self) -> str:
        return self._acx_metadata.axo_source_bucket_id or self.set_source_bucket_id()

    # -- endpoint -------------------------------------------------------
    def set_endpoint_id(self, endpoint_id: str = "") -> str:
        eid = (
            f"axo-endpoint-{_generate_id('', size=8)}" if not endpoint_id else endpoint_id
        )
        self._acx_metadata.axo_endpoint_id = eid
        return eid

    def get_endpoint_id(self) -> str:
        e_id = self._acx_metadata.axo_endpoint_id
        return self.set_endpoint_id(None) if e_id == None else e_id

    # ------------------------------------------------------------------ #
    # Construction magic
    # ------------------------------------------------------------------ #
    def __new__(cls, *args: Any, **kwargs: Any) -> "Axo":
        # print(kwargs)
        obj = super().__new__(cls)
        obj._acx_metadata = MetadataX(
            axo_class_name=cls.__name__, axo_module=cls.__module__
        )
        obj._acx_metadata.axo_bucket_id        = kwargs.get("axo_bucket_id",obj._acx_metadata.axo_bucket_id)
        obj._acx_metadata.axo_source_bucket_id = kwargs.get("axo_source_bucket_id",obj._acx_metadata.axo_source_bucket_id)
        obj._acx_metadata.axo_sink_bucket_id   = kwargs.get("axo_sink_bucket_id",obj._acx_metadata.axo_sink_bucket_id)
        obj._acx_metadata.axo_key              = kwargs.get("axo_key",obj._acx_metadata.axo_key)
        obj._acx_metadata.axo_alias            = kwargs.get("axo_alias",obj._acx_metadata.axo_alias)
        obj._acx_metadata.axo_version          = kwargs.get("axo_version",obj._acx_metadata.axo_version)
        obj._acx_metadata.axo_endpoint_id      = kwargs.get("axo_endpoint_id",obj._acx_metadata.axo_endpoint_id)
        return obj
    
    def __init__(self,*args,**kwargs):
        pass

    # ------------------------------------------------------------------ #
    # Serialisation helpers
    # ------------------------------------------------------------------ #
    def to_bytes(self) -> Result[bytes,Exception]:
        """
        Serialise *attributes*, and
        *class source code* into a single byte‑buffer:

            [len][attrs]  [len][src]
        """
        raw_parts_result = self.get_raw_parts()
        if raw_parts_result.is_err:
            logger.error({"error":str(raw_parts_result.unwrap_err()),"detail":"Failed to get raw parts"})
            return Err(raw_parts_result.unwrap_err())
        
        attrs,class_code = raw_parts_result.unwrap()
        

        parts = [cp.dumps(attrs),  class_code.encode("utf-8")]
        packed = b""
        for part in parts:
            packed += struct.pack("I", len(part)) + part
        return Ok(packed)
    
    def  get_raw_parts(self)->Result[Tuple[Dict[str, Any], str]]:
        try:
            attrs = self.__dict__
            class_code = inspect.getsource(self.__class__)

            return Ok((attrs,   class_code))
        except Exception as e:
            return Err(e)
    
    def to_stream(self,chunk_size: str = "1MB") -> Generator[bytes, None, None]:
        """Yield ``self`` as (roughly) *chunk_size* byte blocks."""
        try:
            serialized_data_res = self.to_bytes()
            if serialized_data_res.is_err:
                raise Exception(f"Failed to convert AO to bytes: {serialized_data_res.unwrap_err()}")
            serialized_data = serialized_data_res.unwrap()

            total_size = len(serialized_data)
            start = 0
            chunk_size = HF.parse_size(chunk_size)
            
            while start < total_size:
                end = min(start + chunk_size, total_size)
                yield serialized_data[start:end]
                start = end
        except Exception as e:
            raise e
        # return serialize_and_yield_chunks(self, chunk_size=chunk_size)
    
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
            # methods = cp.loads(raw_parts[1])
            # class_df = rebuilt_class  # You can also `cp.loads(raw_parts[2])` if needed

            return Ok((attrs, code_str))

        except Exception as e:
            logger.error({"error":str(e), "detail":"Failed to get AO parts"})
            return Err(e)
    # -- Deserialisation -----------------------------------------------
    @staticmethod
    def from_bytes(raw: bytes) -> Result["Axo", Exception]:
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
        rt = get_runtime()
        if rt is None:
            logger.error({"event":"FAILED.NO.RUNTIME"})
            return Err(Exception("No runtime is running."))
        return await rt.get_active_object(key=key, bucket_id=bucket_id)
