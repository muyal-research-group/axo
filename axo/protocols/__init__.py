# axo/types.py
from __future__ import annotations
from typing import Protocol, runtime_checkable, Any, Generator
from option import Result

class MetadataXLike(Protocol):
    axo_key: str
    axo_bucket_id: str
    axo_sink_bucket_id: str
    axo_source_bucket_id: str
    axo_endpoint_id: str
    axo_class_name: str
    axo_module: str
    axo_name: str

@runtime_checkable
class AxoLike(Protocol):
    _acx_metadata: MetadataXLike
    _acx_local: bool
    _acx_remote: bool

    def set_endpoint_id(self, endpoint_id: str = "") -> str: ...
    def get_endpoint_id(self) -> str: ...

    def get_axo_key(self) -> str: ...
    def get_axo_bucket_id(self) -> str: ...
    def get_axo_sink_bucket_id(self) -> str: ...
    def get_axo_source_bucket_id(self) -> str: ...

    async def persistify(self, *, bucket_id: str = "", key: str = "") -> Result[str, Exception]: ...
    @staticmethod
    async def get_by_key(key: str, *, bucket_id: str = "") -> Result["AxoLike", Exception]: ...

    def to_bytes(self) -> bytes: ...
    def to_stream(self, chunk_size: str = "1MB") -> Generator[bytes, None, None]: ...
