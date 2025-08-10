
from pydantic import BaseModel,Field
from typing import ClassVar,Optional,List,Dict,Any
import os
# 
from axo.environment import AXO_ID_SIZE
from axo.models import AxoObjectId
from axo.helpers import _generate_id

# pydantic *Annotated* alias for object keys
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