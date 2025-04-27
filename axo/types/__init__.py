
from typing import TypeVar,Protocol
from axo.endpoint.endpoint import EndpointX
# =========================================================================== #
# Endpoint‑manager *protocol*
# =========================================================================== #
E_co = TypeVar("E_co", bound=EndpointX, covariant=True)
class EndpointManagerP(Protocol[E_co]):
    """
    Minimal contract required by ActiveXRuntime.

    Any concrete manager (local, distributed, …) can satisfy this protocol
    simply by implementing the two methods below.
    """

    def get_endpoint(self, endpoint_id: str = None)->E_co: ...  # noqa: D401 (stub)

    # Used indirectly by `persistify`; the concrete endpoint must have `put`
    # but we don’t prescribe its full signature here.
    # (mypy accepts `Protocol` with incomplete attributes.)
    # def add_endpoint(...): ...
    # def exists(...): ...
    # etc.
