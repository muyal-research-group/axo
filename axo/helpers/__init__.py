
from __future__ import annotations
from typing import Callable,Optional
from nanoid import generate as nanoid
import re
from axo.environment import AXO_ID_SIZE,ALPHABET
from axo.storage.types import AxoObjectBlob,AxoObjectBlobs,AxoStorageMetadata
from axo.errors import AxoError,AxoErrorType
from axo.storage.utils import StorageUtils as SU
from option import Result,Ok,Err
from axo.serde import serialize_attrs

ALLOWED_PATTERN = re.compile(r"[^a-z0-9_]")
UNDERSCORE_PATTERN = re.compile(r"_+")


def _generate_id(val: str | None = None, *, size: int = AXO_ID_SIZE) -> str:
    """
    Return a clean identifier:
    - Only a-z, 0-9, and underscores.
    - Collapse consecutive invalid characters into a single underscore.
    - Strip leading/trailing underscores.
    - If input is None/empty or normalizes to nothing, generate a nanoid.
    """
    if not val:
        return nanoid(alphabet=ALPHABET, size=size)

    # Lowercase everything
    norm = val.lower()

    # Replace invalid chars with underscores
    norm = ALLOWED_PATTERN.sub("_", norm)

    # Collapse multiple underscores
    norm = UNDERSCORE_PATTERN.sub("_", norm)

    # Strip leading/trailing underscores
    norm = norm.strip("_")

    # Fallback if empty after cleaning
    if not norm:
        return nanoid(alphabet=ALPHABET, size=size)

    return norm


def _make_id_validator(size: int) -> Callable[[str | None], str]:
    """Factory that produces a pydantic *AfterValidator* for ID fields."""

    def _validator(v: str | None) -> str:
        return _generate_id(v, size=size)

    return _validator

def _build_axo_uri(axo_bucket_id: str,
                   axo_key:str,
                   class_name: Optional[str]=None,
                   method: Optional[str]=None,
                   axo_version:Optional[int] = 0
) -> Optional[str]:
    # Only build when we have the pieces we need
    if not class_name or not method:
        return f"axo://{axo_bucket_id}:{axo_key}/{axo_version}"
    
    method     = method.strip() if method else ""
    class_name = class_name.strip() if method else ""

    if (method or method != "") and (class_name or class_name != ""):
        return f"axo://{axo_bucket_id}:{axo_key}/{axo_version}?class={class_name}&method={method}"
    elif (method or method != ""):
        return f"axo://{axo_bucket_id}:{axo_key}/{axo_version}?method={method}"
    # No method (e.g., PUT.METADATA): encode just the class
    return f"axo://{axo_bucket_id}:{axo_key}/{axo_version}"

def serialize_blobs_from_instance(instance, *, bucket_id: str, key: str
) -> Result[tuple[AxoObjectBlobs, str], AxoError]:
    """
    Convert an Axo instance into AxoObjectBlobs + class_name.
    Uses instance.get_raw_parts() -> (attrs, class_code_str).
    """
    try:
        raw_parts_res = instance.get_raw_parts()
        if raw_parts_res.is_err:
            return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, str(raw_parts_res.unwrap_err())))
        attrs, class_code_str = raw_parts_res.unwrap()

        # bytes
        src_bytes = class_code_str.encode("utf-8")
        attr_bytes, attr_ct = serialize_attrs(attrs)

        # meta
        src_key = SU.source_key(key)
        attr_key = SU.attrs_key(key)

        producer_id = getattr(getattr(instance, "_acx_metadata", None), "producer_id", "") or "axo"
        tags = instance._acx_metadata.to_tags()
        src_md = AxoStorageMetadata(
            key          = src_key,
            ball_id      = src_key,
            size         = len(src_bytes),
            checksum     = SU.sha256_hex(src_bytes),
            producer_id  = producer_id,
            bucket_id    = bucket_id,
            tags         = {**tags},
            content_type = "text/plain",
            is_disabled  = False,
        )
        attr_md = AxoStorageMetadata(
            key          = attr_key,
            ball_id      = attr_key,
            size         = len(attr_bytes),
            checksum     = SU.sha256_hex(attr_bytes),
            producer_id  = producer_id,
            bucket_id    = bucket_id,
            tags         = {**tags},
            content_type = attr_ct,
            is_disabled  = False,
        )

        blobs = AxoObjectBlobs(
            source_code_blob=AxoObjectBlob(src_bytes, src_md),
            attrs_blob=AxoObjectBlob(attr_bytes, attr_md),
        )
        class_name = getattr(getattr(instance, "_acx_metadata", None), "axo_class_name", None)
        if not class_name:
            return Err(AxoError.make(AxoErrorType.VALIDATION_FAILED, "missing axo_class_name in metadata"))

        return Ok((blobs, class_name))
    except Exception as e:
        return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, f"build_blobs failed: {e}"))


