
from __future__ import annotations
from typing import Callable
import os
from nanoid import generate as nanoid
import string
import re
from axo.environment import AXO_ID_SIZE,ALPHABET

def _generate_id(val: str | None, *, size: int = AXO_ID_SIZE) -> str:
    """Return a valid identifier containing only a‑z 0‑9 _ ."""
    if not val:
        return nanoid(alphabet=ALPHABET, size=size)
    return re.sub(r"[^a-z0-9_]", "", val)
def _make_id_validator(size: int) -> Callable[[str | None], str]:
    """Factory that produces a pydantic *AfterValidator* for ID fields."""

    def _validator(v: str | None) -> str:
        return _generate_id(v, size=size)

    return _validator
