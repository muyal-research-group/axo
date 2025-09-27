
import json as J
from typing import Tuple,Dict,Any

def serialize_attrs( attrs:Dict[str,Any]) -> Tuple[bytes, str]:
    """
    Prefer JSON; fallback to cloudpickle. Returns (bytes, content_type).
    """
    # JSON path
    try:
        b = J.dumps(attrs, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return b, "application/json"
    except Exception:
        pass

    # cloudpickle fallback
    try:
        import cloudpickle as cp  # local import
        return cp.dumps(attrs), "application/x-python-cloudpickle"
    except Exception as e:
        raise RuntimeError(f"attrs not serializable: {e}")
    
