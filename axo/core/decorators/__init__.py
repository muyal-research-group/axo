
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
import time as T
from functools import wraps
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    TypeVar,
    cast,
    Dict, 
    Literal, 
    Optional,
    Union
)

# ─────────────────────────────────────────────────────────────── 3rd‑party ──
from nanoid import generate as nanoid
from option import Err, Ok, Result
import wrapt

# ──────────────────────────────────────────────────────────────── project ───
# 
from axo.log import get_logger
# 
# from axo.storage.data import LocalStorageService
# 
# from axo.runtime.local import LocalRuntime
from axo.runtime import set_runtime,get_runtime
from axo.protocols import AxoLike
from axo.errors import AxoError,AxoErrorType
from axo.core.models import AxoContext,DeserializeT,AckT
# from axo.endpoint.endpoint import EndpointX

logger = get_logger(name=__name__)
R = TypeVar("R")  # generic return type for Axo.call

# ---------------------------------------------------------------------------
# Config carried by the decorated method (runtime will read this)
# ---------------------------------------------------------------------------


    

# ---------------------------------------------------------------------------
# Public decorators
# ---------------------------------------------------------------------------

def axo_task(
    *,
    source_bucket: Optional[str] = "",
    sink_bucket: Optional[str] = "",
    filter_tags: Optional[Dict[str, str]] = None,
    filter_prefix: Optional[str] = None,
    deserialize: DeserializeT = "bytes",
    ack: AckT = "delete",
    lease_seconds: int = 60,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorate a method to consume **one** item from a bucket when invoked by the runtime.
    Includes `@axo_method` behavior automatically; do not stack both.
    """
    ctx = AxoContext(
        kind          = "task",
        source_bucket = source_bucket,
        sink_bucket   = sink_bucket,
        filter_tags   = filter_tags,
        filter_prefix = filter_prefix,
        deserialize   = deserialize,
        ack           = ack,
        lease_seconds = lease_seconds,
    )

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        wrapped = __axo_task(fn,ctx=ctx)          # preserve normal Axo method semantics
        return wrapped
        # return _attach_config(wrapped, cfg)
    return decorator





def axo_stream(
    *,
    source_bucket: str,
    filter_tags: Optional[Dict[str, str]] = None,
    filter_prefix: Optional[str] = None,
    deserialize: DeserializeT = "bytes",
    ack: AckT = "delete",
    lease_seconds: int = 60,
    # stream knobs
    parallel: int = 1,
    batch: int = 16,
    max_items: Optional[int] = None,
    max_seconds: Optional[int] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorate a method to consume **many** items (optionally in parallel) from a bucket.
    Includes `@axo_method` behavior automatically; do not stack both.
    """
    cfg = AxoContext(
        kind="stream",
        source_bucket=source_bucket,
        filter_tags=filter_tags,
        filter_prefix=filter_prefix,
        deserialize=deserialize,
        ack=ack,
        lease_seconds=lease_seconds,
        parallel=parallel,
        batch=batch,
        max_items=max_items,
        max_seconds=max_seconds,
    )

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        wrapped = __axo_stream(fn)
        return wrapped
        # return _attach_config(wrapped, cfg)
    return decorator


def __axo_stream(wrapped: Callable[..., R]) -> Callable[..., Result[R, Exception]]:
    @wrapt.decorator
    def _wrapper(wrapped_func, instance:AxoLike, args, kwargs):
        try:
            t1 = T.time()
            rt = get_runtime()
            if rt is None:
                logger.warning({"event":"RUNTIME.NOT.STARTED", "mode":"LOCAL"})
                return Err(AxoError.make(error_type=AxoErrorType.INTERNAL_ERROR, msg="No runtime started"))
                # set_runtime(_make_local_runtime())
                # rt = get_runtime()



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
            # if res.is_ok:S
                # result = res.unwrap()
                # if isinstance(result, Result):
                    # return Ok(result.unwrap())
            return res
        except Exception as e:
            logger.error(f"METHOD.EXEC failed: {e}")
            return Err(e)

    return cast(Callable[..., Result[R, Exception]],_wrapper(wrapped))

def __axo_task(wrapped: Callable[..., R], ctx:AxoContext) -> Callable[..., Result[R, Exception]]:
    @wrapt.decorator
    def _wrapper(wrapped_func, instance:AxoLike, args, kwargs):
        try:
            t1 = T.time()
            rt = get_runtime()
            if rt is None:
                logger.warning({"event":"RUNTIME.NOT.STARTED", "mode":"LOCAL"})
                return Err(AxoError.make(error_type=AxoErrorType.INTERNAL_ERROR, msg="No runtime started"))
                # set_runtime(_make_local_runtime())
                # rt = get_runtime()



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
            

            fname:str = wrapped_func.__name__
            

            res = ep.task_execution(
                fname = fname,
                ao    = instance,
                ctx   = ctx
            )
            print("RES",res)
            

            logger.info({
                "event": "TASK.EXEC",
                "mode":"DISTRIBUTED" if rt.is_distributed else "LOCAL",
                "fname": fname,
                # "ok": res.is_ok,
                "response_time": T.time() - t1,  # update this properly
            })
            return Ok(1)
            # if res.is_ok:S
                # result = res.unwrap()
                # if isinstance(result, Result):
                    # return Ok(result.unwrap())
            # return res
        except Exception as e:
            logger.error(f"METHOD.EXEC failed: {e}")
            return Err(e)

    return cast(Callable[..., Result[R, Exception]],_wrapper(wrapped))


def axo_method(wrapped: Callable[..., R]) -> Callable[..., Result[R, Exception]]:
    @wrapt.decorator
    def _wrapper(wrapped_func, instance:AxoLike, args, kwargs):
        try:
            t1 = T.time()
            rt = get_runtime()
            if rt is None:
                logger.warning({"event":"RUNTIME.NOT.STARTED", "mode":"LOCAL"})
                return Err(AxoError.make(error_type=AxoErrorType.INTERNAL_ERROR, msg="No runtime started"))
                # set_runtime(_make_local_runtime())
                # rt = get_runtime()



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
            print(instance.get_axo_bucket_id(), instance.get_axo_key())
            res = ep.method_execution(
                key     = instance.get_axo_key(),
                fname   = fname,
                ao      = instance,
                fargs   = args,
                fkwargs = kwargs,
            )
            # print("RES",res)
            logger.info({
                "event": "METHOD.EXEC",
                "mode":"DISTRIBUTED" if rt.is_distributed else "LOCAL",
                "fname": fname,
                "ok": res.is_ok,
                "response_time": T.time() - t1,  # update this properly
            })
            # if res.is_ok:S
                # result = res.unwrap()
                # if isinstance(result, Result):
                    # return Ok(result.unwrap())
            return res
        except Exception as e:
            logger.error(f"METHOD.EXEC failed: {e}")
            return Err(e)

    return cast(Callable[..., Result[R, Exception]],_wrapper(wrapped))

