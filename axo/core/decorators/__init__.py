
"""
axo/core/axo.py
~~~~~~~~~~~~~~~

Core building blocks for the *Axo* programming model:

*   **Decorators**

    * :func:`axo_method` – transparently routes a *synchronous* method call
      through the active‑object runtime (local or distributed).

*   **Pydantic metadata model** – :class:`MetadataX`.

*   **Base active‑object class** – :class:`Axo`.

The module relies on `option` (``Ok`` / ``Err`` / ``Result``) for explicit
error handling and uses *cloudpickle* to (de)serialise user‑defined classes.
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────── stdlib ──
import string
import time as T
from functools import wraps
from typing import (
    Any,
    Callable,
    TypeVar,
    cast
)

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
from nanoid import generate as nanoid
from option import Err, Ok, Result
import wrapt

# ──────────────────────────────────────────────────────────────── project ───
# 
from axo.log import get_logger
# 
from axo.storage.data import LocalStorageService
# 
from axo.runtime.local import LocalRuntime
from axo.runtime import set_runtime,get_runtime
from axo.protocols import AxoLike

logger = get_logger(name=__name__)
R = TypeVar("R")  # generic return type for Axo.call
# =========================================================================== #
# Decorators
# =========================================================================== #
# @wrapt.decorator
def _make_local_runtime() -> LocalRuntime:
    suffix = nanoid(alphabet=string.ascii_lowercase + string.digits)
    return LocalRuntime(
        runtime_id=f"local-{suffix}",
        storage_service=LocalStorageService(storage_service_id=f"local-storage-{suffix}")
    )

def axo_method(wrapped: Callable[..., R]) -> Callable[..., Result[R, Exception]]:
    @wrapt.decorator
    def _wrapper(wrapped_func, instance:AxoLike, args, kwargs):
        try:
            t1 = T.time()
            rt = get_runtime()
            if rt is None:
                logger.warning({"event":"RUNTIME.NOT.STARTED", "mode":"LOCAL"})
                set_runtime(_make_local_runtime())
                rt = get_runtime()



            e_id = kwargs.get("axo_endpoint_id", instance.get_endpoint_id()) 

            ep = rt.endpoint_manager.get_endpoint(e_id)
            
            instance.set_endpoint_id(ep.endpoint_id)

            kwargs.setdefault("axo_endpoint_id", ep.endpoint_id)
            kwargs.setdefault("axo_key", instance.get_axo_key())
            kwargs.setdefault("axo_bucket_id", instance.get_axo_bucket_id())
            kwargs.setdefault("axo_sink_bucket_id", instance.get_axo_sink_bucket_id())
            kwargs.setdefault("axo_source_bucket_id", instance.get_axo_source_bucket_id())
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

