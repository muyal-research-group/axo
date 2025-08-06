from __future__ import annotations
import asyncio
import inspect
import functools
from typing import Callable,Awaitable,TypeVar,Any,Optional
from typing_extensions import ParamSpec,overload     # type: ignore

P = ParamSpec("P")   # “parameter pack” of the wrapped function

R = TypeVar("R")
S = TypeVar("S")                # return type of on_skip()  (may differ)

@overload
def guard_if_failed(
    *, 
    on_skip: None = None,                 #   no custom fallback
    log: Optional[Callable[[str], None]] = None,
) -> Callable[[Callable[P, R]], Callable[P, Optional[R]]]: ...
#                          ↑              ↑­­­­­­­­­ optional !───┘

@overload
def guard_if_failed(
    *, 
    on_skip: Callable[[Any], S],          #   user supplies fallback
    log: Optional[Callable[[str], None]] = None,
) -> Callable[[Callable[P, R]], Callable[P, S]]: ...

def guard_if_failed(
    *,
    on_skip: Callable[[Any], Any] | None = None,   # fallback result
    log: Callable[[str], None] | None = None,    # logger sink
) -> Callable[[Callable[P, R]], Callable[P, Any]]:
    """
    Skip a method when `self.is_failed` is True **without losing its signature**.
    Works on both sync and async functions and on Python 3.7 → 3.13.
    """
    log = log or print

    def decorator(func: Callable[P, R]) -> Callable[P, Any]:
        is_async = inspect.iscoroutinefunction(func)

        if is_async:                                # ---------- async case ----------
            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kw: P.kwargs) -> R:  # type: ignore
                self_obj = args[0]
                if getattr(self_obj, "is_failed", False):
                    log(f"[SKIPPED] {func.__qualname__}")
                    if on_skip:
                        return on_skip(self_obj)
                    return await asyncio.sleep(0, result=None)  # type: ignore[return-value]
                return await func(*args, **kw)

            async_wrapper.__signature__ = inspect.signature(func)  # runtime IntelliSense
            return async_wrapper  # type: ignore[return-value]

        else:                                        # ---------- sync case ----------
            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kw: P.kwargs) -> R:  # type: ignore
                self_obj = args[0]
                if getattr(self_obj, "is_failed", False):
                    log(f"[SKIPPED] {func.__qualname__}")
                    return on_skip(self_obj) if on_skip else None  # type: ignore[return-value]
                return func(*args, **kw)

            sync_wrapper.__signature__ = inspect.signature(func)
            return sync_wrapper  # type: ignore[return-value]

    return decorator
