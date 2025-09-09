
from __future__ import annotations
from typing import Callable,Optional
import os
from nanoid import generate as nanoid
import string
import re
from axo.environment import AXO_ID_SIZE,ALPHABET

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
