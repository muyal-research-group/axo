from axo.storage.types import ComposedKey,AxoStorageMetadata,AxoObjectBlob
from axo.storage.constants import AXO_ATTRS_SUFFIX,AXO_SOURCE_CODE_SUFFIX
from typing import Dict,Optional
import hashlib as H

class StorageUtils:
    @staticmethod
    def source_key(key: ComposedKey) -> ComposedKey:
        return f"{key}_source_code"
    @staticmethod
    def attrs_key(key: ComposedKey) -> ComposedKey:
        return f"{key}_attrs"
    @staticmethod
    def sha256_hex(data: bytes) -> str:
        return H.sha256(data).hexdigest()
    @staticmethod
    def to_tags(meta: AxoStorageMetadata, extra: Dict[str, str]  = {}) -> Dict[str, str]:
        tags = dict(meta.tags or {})
        tags.update({
            "checksum": meta.checksum,
            "size": str(meta.size),
            "producer_id": meta.producer_id,
            "content_type": meta.content_type or "application/octet-stream",
            "ball_id": meta.ball_id,
        })
        if extra:
            tags.update(extra)
        return tags
    
    @staticmethod
    def source_key(key: ComposedKey) -> ComposedKey:
        # prefer your env-driven suffixes
        # (kept for compatibility with StorageUtils)
        return f"{key}_{AXO_SOURCE_CODE_SUFFIX}"

    @staticmethod
    def attrs_key(key: ComposedKey) -> ComposedKey:
        return f"{key}_{AXO_ATTRS_SUFFIX}"
    
    # ----- validations -----
    @staticmethod
    def _validate_blob_integrity(blob: AxoObjectBlob) -> Optional[str]:
        if len(blob.data) != blob.metadata.size:
            return "size mismatch"
        if StorageUtils.sha256_hex(blob.data) != blob.metadata.checksum:
            return "checksum mismatch"
        return None