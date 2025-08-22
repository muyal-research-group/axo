import random 
from typing import Dict
from axo.endpoint.endpoint import LocalEndpoint,DistributedEndpoint
from abc import ABC 
import logging
from axo.types import EndpointManagerP

logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(_h)
logger.setLevel(logging.DEBUG)
# ──────────────────────────────────────────────────────────────────────────────
# Manager base‑class (unchanged)
# ──────────────────────────────────────────────────────────────────────────────
class _BaseEndpointManager(ABC):
    """Common convenience helpers shared by both concrete managers."""

    def __init__(self, *, endpoint_manager_id: str = "") -> None:
        self.endpoint_manager_id = endpoint_manager_id
        self._rr_counter = 0  # used by round‑robin fallback

    # ------------------------------------------------------------------ #
    # Utility
    # ------------------------------------------------------------------ #
    @staticmethod
    def _random_port(low: int = 16000, high: int = 65000) -> int:
        return random.randint(low, high)


# ============================================================================ #
# 1. LOCAL  Endpoint manager
# ============================================================================ #
class LocalEndpointManager(_BaseEndpointManager,EndpointManagerP[LocalEndpoint]):
    """
    Registry that ONLY handles :class:`LocalEndpoint` objects.

    Typical usage (unit tests, single‑machine runs)::

        mgr = LocalEndpointManager()
        mgr.add_endpoint(endpoint_id="local‑0")      # one is enough
        ep = mgr.get_endpoint()                      # always returns the same
    """

    def __init__(self, endpoint_manager_id: str = "axo-local-endpoint") -> None:
        super().__init__(endpoint_manager_id=endpoint_manager_id)
        self.endpoints: Dict[str, LocalEndpoint] = {}
        self.n = 0

    # ---------------------------- CRUD ---------------------------------
    def add_endpoint(self, *, endpoint_id: str) -> None:
        self.endpoints[endpoint_id] = LocalEndpoint(endpoint_id=endpoint_id)

    def del_endpoint(self, endpoint_id: str) -> LocalEndpoint :
        return self.endpoints.pop(endpoint_id, None)

    def exists(self, endpoint_id: str) -> bool:
        return endpoint_id in self.endpoints

    def get_endpoint(self, endpoint_id: str = "") -> LocalEndpoint:
        # Always return the requested ID (or the *only* one we have)
        if len(self.endpoints) ==0:
            return None
        if endpoint_id:
            self.n+= 1 
            return self.endpoints.get(endpoint_id,None)
        
        return list(self.endpoints.values())[self.n & len(self.endpoints)]
        # next(iter(self.endpoints.values()))  # first/only element


# ============================================================================ #
# 2. DISTRIBUTED Endpoint manager  (keeps round‑robin behaviour)
# ============================================================================ #
class DistributedEndpointManager(_BaseEndpointManager,EndpointManagerP[DistributedEndpoint]):
    """
    Registry for :class:`DistributedEndpoint` objects.

    * `get_endpoint()` cycles through the list if no ID is provided
      (simple round‑robin load‑balancing).
    * Port helpers guarantee we never re‑use a port already allocated
      in the same manager instance.
    """

    def __init__(
        self,
        endpoints: Dict[str, DistributedEndpoint]= {},
        endpoint_manager_id: str = "",
    ) -> None:
        super().__init__(endpoint_manager_id=endpoint_manager_id)
        self.endpoints: Dict[str, DistributedEndpoint] = endpoints 

    # ---------------------------- CRUD ---------------------------------
    def add_endpoint(
        self,
        *,
        endpoint_id: str,
        hostname: str,
        pubsub_port: int,
        req_res_port: int,
        protocol: str = "tcp",
    ) -> None:
        ep = DistributedEndpoint(
            endpoint_id=endpoint_id,
            hostname=hostname,
            pubsub_port=pubsub_port,
            req_res_port=req_res_port,
            protocol=protocol,
        )
        # Try a ping upfront so we fail fast if unreachable
        if endpoint_id != self.endpoint_manager_id and not ep._ensure_connection():
            logger.error(
                {
                    "event": "ENDPOINT.UNREACHABLE",
                    "endpoint_id": endpoint_id,
                    "hostname": hostname,
                    "req_res_port": req_res_port,
                }
            )
        self.endpoints[endpoint_id] = ep

    def del_endpoint(self, endpoint_id: str) -> DistributedEndpoint :
        return self.endpoints.pop(endpoint_id, None)

    def exists(self, endpoint_id: str) -> bool:
        return endpoint_id in self.endpoints

    def get_endpoint(self, endpoint_id: str = "") -> DistributedEndpoint:
        if endpoint_id and endpoint_id in self.endpoints:
            return self.endpoints[endpoint_id]
        # Round‑robin fallback
        ep = list(self.endpoints.values())[self._rr_counter % len(self.endpoints)]
        self._rr_counter += 1
        return ep

    # -------------------------- Port helpers ---------------------------
    def get_available_req_res_port(self) -> int:
        used = {ep.req_res_port for ep in self.endpoints.values()}
        port = self._random_port()
        while port in used:
            port = self._random_port()
        return port

    def get_available_pubsub_port(self) -> int:
        used = {ep.pubsub_port for ep in self.endpoints.values()}
        port = self._random_port()
        while port in used:
            port = self._random_port()
        return port