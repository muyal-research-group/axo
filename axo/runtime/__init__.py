"""
axo/runtime/__init__.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Utility helpers that keep track of **which Axo runtime is currently active**
(`LocalRuntime` for single‑process execution or `DistributedRuntime` for
cluster/serverless execution).

Key ideas
---------
* We expose two tiny helpers, `get_runtime()` and `set_runtime()`, so the rest of
  the codebase never touches a global directly.
* A `ContextVar` is used for *session‑level* information that must remain
  isolated per coroutine / thread (e.g. a request ID or tenant ID).
* Static‑type checkers (MyPy, Pyright, Pylance) get full type information
  without introducing a real import at runtime, avoiding circular‑import
  headaches.

"""

from __future__ import annotations  # allow forward references without quotes

from typing import Union, TYPE_CHECKING
import contextvars

# ──────────────────────────────────────────────────────────────────────────────
# Conditional imports for static‑type checkers only
# They are ignored at runtime → zero overhead / no circular import problems.
# ──────────────────────────────────────────────────────────────────────────────
if TYPE_CHECKING:
    from axo.runtime.local import LocalRuntime
    from axo.runtime.distributed import DistributedRuntime

# ──────────────────────────────────────────────────────────────────────────────
# Context‑local storage
# Each asyncio task / OS thread will get its *own* copy of this variable.
# Perfect for request‑scoped or tenant‑scoped data.
# ──────────────────────────────────────────────────────────────────────────────
current_session: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_session", default=None
)

# ──────────────────────────────────────────────────────────────────────────────
# Global pointer to the active runtime backend
# NOTE: we keep it simple here; if you need *per‑task* runtimes, wrap this in
# another ContextVar just like `current_session`.
# ──────────────────────────────────────────────────────────────────────────────
_current_runtime: Union["LocalRuntime", "DistributedRuntime", None] = None
_runtime_cv = contextvars.ContextVar("axo_runtime", default=None)


# ──────────────────────────────────────────────────────────────────────────────
# Public helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_runtime() -> Union["LocalRuntime", "DistributedRuntime", None]:
    """
    Return the runtime currently in use.

    • `LocalRuntime`  → single‑process / in‑memory execution
    • `DistributedRuntime` → remote or serverless execution
    • `None` → runtime not configured yet (startup phase)
    """
    return _runtime_cv.get()


def set_runtime(runtime: Union["LocalRuntime", "DistributedRuntime"]) -> None:
    """
    Set the active runtime.

    This should be called once during application bootstrap, e.g.:

    ```python
    if settings.distributed_mode:
        set_runtime(DistributedRuntime(config))
    else:
        set_runtime(LocalRuntime())
    ```

    Parameters
    ----------
    runtime : LocalRuntime | DistributedRuntime
        The backend that will handle task scheduling, storage, etc.
    """
    _runtime_cv.set(runtime)
    # global _current_runtime
    # _current_runtime = runtime