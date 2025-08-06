"""
axo/endpoint/endpoint.py
~~~~~~~~~~~~~~~

Endpoint abstractions for Axo.

* ``EndpointX``   – abstract interface that defines how runtimes talk to a
  metadata / code‑execution endpoint (local or distributed).
* ``LocalEndpoint`` – in‑process stub used for unit tests or single‑node mode.
* ``DistributedEndpoint`` – ZeroMQ‑based client that talks to a remote Axo
  middleware.
* ``EndpointManagerX`` – simple round‑robin registry that keeps track of all
  available endpoints and hands them out to callers.

The module uses :pymod:`option` (Ok / Err / Result) for explicit error handling.
"""

from __future__ import annotations

import json
import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict,  TypeVar

import cloudpickle as cp
import humanfriendly as hf
import zmq

from axo import Axo
from axo.storage.metadata import MetadataX
from option import Err, Ok, Result

# ──────────────────────────────────────────────────────────────────────────────
# Typing aliases
# ──────────────────────────────────────────────────────────────────────────────
T = TypeVar("T")
GenericFunction = Callable[[T], T]

# ──────────────────────────────────────────────────────────────────────────────
# Logger
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(_h)
logger.setLevel(logging.DEBUG)


# ============================================================================
# Abstract base endpoint
# ============================================================================
class EndpointX(ABC):
    """
    Generic endpoint interface.

    A concrete endpoint must implement CRUD operations for metadata,
    remote method execution, code upload, and elasticity control.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        *,
        protocol: str = "tcp",
        hostname: str = "127.0.0.1",
        req_res_port: int = 60667,
        pubsub_port: int = 16666,
        encoding: str = "utf-8",
        endpoint_id: str = "axo-endpoint-0",
        is_local: bool = True,
    ) -> None:
        self.endpoint_id: str = endpoint_id
        self.protocol: str = protocol
        self.hostname: str = hostname
        self.req_res_port: int = req_res_port
        self.pubsub_port: int = pubsub_port
        self.encoding: str = encoding
        self.is_local: bool = is_local

    # ------------------------------------------------------------------ #
    # CRUD + RPC interface
    # ------------------------------------------------------------------ #
    @abstractmethod
    def put(self, key: str, value: MetadataX) -> Result[str, Exception]: ...

    @abstractmethod
    def get(self, key: str) -> Result[MetadataX, Exception]: ...

    @abstractmethod
    def method_execution(
        self,
        *,
        key: str,
        fname: str,
        ao: Axo,
        f: GenericFunction | None = None,
        fargs: list[Any] | None = None,
        fkwargs: dict[str, Any] | None = None,
    ) -> Result[Any, Exception]: ...

    @abstractmethod
    def add_code(self, ao: Axo) -> Result[bool, Exception]: ...

    @abstractmethod
    def add_class_definition(
        self, class_def: Any, *, bucket_id: str = "", key: str = ""
    ) -> Result[bool, Exception]: ...

    @abstractmethod
    def task_execution(
        self, task_function: GenericFunction, *, payload: Dict[str, Any] | None = None
    ) -> Result[Any, Exception]: ...

    @abstractmethod
    def elasticity(self, rf: int) -> Result[Any, Exception]: ...


# ============================================================================
# Local stub (in‑memory)
# ============================================================================
class LocalEndpoint(EndpointX):
    """Simple in‑memory endpoint for tests or local mode."""

    def __init__(self, endpoint_id: str = "axo-endpoint-0") -> None:
        super().__init__(endpoint_id=endpoint_id)
        self._db: Dict[str, MetadataX] = {}

    # -- CRUD ------------------------------------------------------------
    def put(self, key: str, value: MetadataX) -> Result[str, Exception]:
        self._db.setdefault(key, value)
        return Ok(key)

    def get(self, key: str) -> Result[MetadataX, Exception]:
        return Ok(self._db[key]) if key in self._db else Err(Exception("Not found"))

    # -- Remote method execution (trivial in local mode) -----------------
    def method_execution(
        self,
        *,
        key: str,
        fname: str,
        ao: Axo,
        f: GenericFunction | None = None,
        fargs: list[Any] | None = None,
        fkwargs: dict[str, Any] | None = None,
    ) -> Result[Any, Exception]:
        try:
            fargs = fargs or []
            fkwargs = fkwargs or {}
            print(fargs)
            # print(f(ao,1,1))
            # return Ok(f(ao, *fargs, **fkwargs)) if f else Err(Exception("No function"))
            return Ok(f(*fargs, **fkwargs)) if f else Err(Exception("No function"))
        except Exception as exc:
            return Err(exc)

    # -- Code / class upload (no‑op in local) ----------------------------
    def add_code(self, ao: Axo) -> Result[bool, Exception]:
        return Ok(False)

    def add_class_definition(
        self, class_def: Any, *, bucket_id: str = "", key: str = ""
    ) -> Result[bool, Exception]:
        return Ok(False)

    # -- Misc ------------------------------------------------------------
    def task_execution(
        self, task_function: GenericFunction, *, payload: Dict[str, Any] | None = None
    ) -> Result[Any, Exception]:
        return Err(Exception("Task execution not supported in LocalEndpoint"))

    def elasticity(self, rf: int) -> Result[Any, Exception]:
        return Err(Exception("Elasticity not applicable in LocalEndpoint"))


# ============================================================================
# Distributed endpoint (ZeroMQ client)
# ============================================================================
class DistributedEndpoint(EndpointX):
    """
    Endpoint that communicates with a remote Axo middleware via ZeroMQ.

    * REQ/REP socket for RPC (metadata, code upload, elasticity, etc.).
    * PUB/SUB socket (future use) for streaming data or events.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        *,
        endpoint_id: str = "axo-endpoint",
        protocol: str = "tcp",
        hostname: str = "127.0.0.1",
        publisher_hostname: str = "*",
        pubsub_port: int = 16666,
        req_res_port: int = 16667,
        max_health_ping_time: str = "1h",
        max_recv_timeout: str = "120s",
        max_retries: int = 10,
    ) -> None:
        super().__init__(
            protocol=protocol,
            hostname=hostname,
            req_res_port=req_res_port,
            pubsub_port=pubsub_port,
            endpoint_id=endpoint_id,
            is_local=False,
        )

        # Build URIs
        self.pubsub_uri = (
            f"{protocol}://{publisher_hostname}:{pubsub_port+1}"
            if pubsub_port != -1
            else f"{protocol}://{publisher_hostname}"
        )
        self.reqres_uri = (
            f"{protocol}://{hostname}:{req_res_port}"
            if req_res_port != -1
            else f"{protocol}://{hostname}"
        )

        # ZMQ sockets
        self._ctx: zmq.Context | None = None
        self._req: zmq.Socket | None = None

        # Health / retry
        self._last_ping_at: float = -1
        self._ping_interval = hf.parse_timespan(max_health_ping_time)
        self._max_retries = max_retries
        self._recv_timeout_ms = int(hf.parse_timespan(max_recv_timeout) * 1000)
        self._connected = False

    # ------------------------------------------------------------------ #
    # Connection management
    # ------------------------------------------------------------------ #
    def _ensure_connection(self) -> bool:
        """Try to (re)connect the REQ socket. Returns True on success."""
        tries = 0
        while not self._connected and tries < self._max_retries:
            try:
                if self._ctx is None:
                    self._ctx = zmq.Context()
                if self._req is None:
                    self._req = self._ctx.socket(zmq.REQ)
                    self._req.setsockopt(zmq.RCVTIMEO, self._recv_timeout_ms)
                    self._req.connect(self.reqres_uri)

                # Ping
                if (
                    self._last_ping_at == -1
                    or time.time() - self._last_ping_at >= self._ping_interval
                ):
                    self._req.send_multipart([b"axo", b"PING", b"{}"])
                    _ = self._req.recv_multipart()
                    self._last_ping_at = time.time()
                    self._connected = True
                    logger.debug("Connected to %s", self.reqres_uri)
            except Exception as exc:
                logger.warning("Retry %d/%d: %s", tries + 1, self._max_retries, exc)
                self._cleanup()
            finally:
                tries += 1
        return self._connected

    def _cleanup(self) -> None:
        """Close sockets & context on error."""
        if self._req is not None:
            self._req.close(linger=1)
        if self._ctx is not None:
            self._ctx.destroy()
        self._req = None
        self._ctx = None
        self._connected = False


    def ping(self):
        try:
            if not self._ensure_connection():
                return Err(Exception("Unable to connect"))
            self._req.send_multipart([b"axo", b"PING", b"{}"])
            _ = self._req.recv_multipart()
            return Ok(True)
        except Exception as e:
            return Err(e)
    # ------------------------------------------------------------------ #
    # CRUD
    # ------------------------------------------------------------------ #
    def put(self, key: str, value: MetadataX) -> Result[str, Exception]:
        if not self._ensure_connection():
            return Err(Exception("Unable to connect"))

        payload = json.dumps(value.model_dump()).encode(self.encoding)
        try:
            self._req.send_multipart([b"axo", b"PUT.METADATA", payload])
            _ = self._req.recv_multipart()
            return Ok(key)
        except Exception as exc:
            self._cleanup()
            return Err(exc)

    def get(self, key: str) -> Result[MetadataX, Exception]:
        return Err(Exception("GET not implemented yet"))

    # ------------------------------------------------------------------ #
    # Remote method execution
    # ------------------------------------------------------------------ #
    def method_execution(
        self,
        *,
        key: str,
        fname: str,
        ao: Axo,
        f: GenericFunction | None = None,
        fargs: list[Any] | None = None,
        fkwargs: dict[str, Any] | None = None,
    ) -> Result[Any, Exception]:
        if not self._ensure_connection():
            return Err(Exception("Unable to connect"))

        payload = json.dumps({"key": key, "fname": fname}).encode(self.encoding)
        try:
            # print("METHO EXECUTION!",fkwargs,fargs)
            if "storage" in fkwargs:
                del fkwargs["storage"]
                
            self._req.send_multipart(
                [
                    b"axo",
                    b"METHOD.EXEC",
                    payload,
                    # cp.dumps(f),
                    cp.dumps(fargs or []),
                    cp.dumps(fkwargs or {}),
                ]
            )
            resp = self._req.recv_multipart()
            if len(resp) != 5:
                return Err(Exception("Unexpected response length"))
            _, _, status_b, meta_b, result_b = resp
            status = int.from_bytes(status_b, "little", signed=True)
            if status < 0:
                return Err(Exception("Remote execution failed"))
            return Ok(self._deserialize(result_b))
        except Exception as exc:
            self._cleanup()
            return Err(exc)

    # ------------------------------------------------------------------ #
    # Code / class upload
    # ------------------------------------------------------------------ #
    def add_code(self, ao: Axo) -> Result[bool, Exception]:
        return Err(Exception("ADD.CODE not implemented yet"))

    def add_class_definition(
        self, class_def: Any, *, bucket_id: str = "", key: str = ""
    ) -> Result[bool, Exception]:
        return Err(Exception("ADD.CLASS.DEF not implemented yet"))

    # ------------------------------------------------------------------ #
    # Task & elasticity helpers
    # ------------------------------------------------------------------ #
    def task_execution(
        self, task_function: GenericFunction, *, payload: Dict[str, Any] | None = None
    ) -> Result[Any, Exception]:
        return Err(Exception("TASK.EXEC not implemented yet"))

    def elasticity(self, rf: int) -> Result[Any, Exception]:
        return Err(Exception("ELASTICITY not implemented yet"))

    # ------------------------------------------------------------------ #
    # Utility
    # ------------------------------------------------------------------ #
    @staticmethod
    def _deserialize(x: bytes) -> Any:
        """Try cloudpickle first; fallback to JSON."""
        try:
            return cp.loads(x)
        except Exception:
            return json.loads(x)

    # String helpers -----------------------------------------------------
    def to_string(self) -> str:
        return f"{self.endpoint_id}:{self.protocol}:{self.hostname}:{self.req_res_port}:{self.pubsub_port}"

    @staticmethod
    def from_str(endpoint_str: str) -> "DistributedEndpoint":
        eid, proto, host, req, pub = endpoint_str.split(":")
        return DistributedEndpoint(
            endpoint_id=eid,
            protocol=proto,
            hostname=host,
            req_res_port=int(req),
            pubsub_port=int(pub),
        )

