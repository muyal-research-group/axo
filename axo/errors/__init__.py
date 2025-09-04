from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from axo.enums import AxoErrorType,AxoErrorCode


class AxoError(BaseModel):
    type: AxoErrorType
    code: int
    message: str
    suggestion: Optional[str] = None
    retry_after_ms: Optional[int] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    causes: List[Dict[str, Any]] = Field(default_factory=list)  # optional cause chain

    @classmethod
    def make(cls, error_type: AxoErrorType, msg: str, *,
             suggestion: str | None = None, retry_after_ms: int | None = None,
             context: Dict[str, Any] | None = None, causes: List[Dict[str, Any]] | None = None,
             code: int | None = None) -> "AxoError":
        return cls(
            type=error_type,
            code=code if code is not None else _default_code(error_type),
            message=msg,
            suggestion=suggestion,
            retry_after_ms=retry_after_ms,
            context=context or {},
            causes=causes or [],
        )

def _default_code(et: AxoErrorType) -> int:
    mapping = {
        AxoErrorType.BAD_REQUEST:              AxoErrorCode.BAD_REQUEST,
        AxoErrorType.UNKNOWN_OPERATION:        AxoErrorCode.UNKNOWN_OPERATION,
        AxoErrorType.VALIDATION_FAILED:        AxoErrorCode.VALIDATION_FAILED,
        AxoErrorType.NOT_FOUND:  AxoErrorCode.NOT_FOUND,
        AxoErrorType.CONCURRENCY_CONFLICT:     AxoErrorCode.CONCURRENCY_CONFLICT,
        AxoErrorType.TIMEOUT:                  AxoErrorCode.TIMEOUT,
        AxoErrorType.STORAGE_ERROR:            AxoErrorCode.STORAGE_ERROR,
        AxoErrorType.DEPENDENCY_INSTALL_FAIL:  AxoErrorCode.DEP_INSTALL_FAIL,
        AxoErrorType.ENDPOINT_COLD:            AxoErrorCode.ENDPOINT_COLD,
        AxoErrorType.NOT_LEADER:               AxoErrorCode.NOT_LEADER,
        AxoErrorType.TRANSPORT_ERROR:          AxoErrorCode.TRANSPORT_ERROR,
        AxoErrorType.INTERNAL_ERROR:           AxoErrorCode.INTERNAL_ERROR,
    }
    return int(mapping.get(et, AxoErrorCode.INTERNAL_ERROR))
