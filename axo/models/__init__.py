from __future__ import annotations
import json as J
import time as T
from typing import Any, Dict, List, Optional,Annotated,Tuple
# 
import cloudpickle as CP
from pydantic import BaseModel, Field, ValidationError, field_validator,AfterValidator,model_validator
from option import Ok, Err, Result
import humanfriendly as HF
# 
from axo.helpers import _make_id_validator,_generate_id,_build_axo_uri
from axo.core.models import MetadataX,AxoContext
from axo.environment import AXO_ID_SIZE
from axo.errors import AxoError,AxoErrorType
from axo.enums import TaskStatus,AxoOperationType
from axo.core.constants import *

AxoObjectId = Annotated[Optional[str], AfterValidator(_make_id_validator(AXO_ID_SIZE))]



class Task:
    def __init__(self,
        operation:str,
        executed_at:float = -1,
        max_waiting_time:str = "1m",
        metadata:Dict[str,str]={}
    ) -> None:
        self.id = _generate_id()
        self.created_at = T.time()
        if executed_at < self.created_at :
            self.executes_at = self.created_at
        else:
            self.executes_at  = self.created_at if executed_at < 0 else executed_at
        self.waiting_time = 0
        self.operation= operation
        self.metadata = metadata
        self.max_waiting_time = HF.parse_timespan(max_waiting_time)
        self.status = TaskStatus.PENDING
    def get_formatted_max_waiting_time(self):
        return HF.format_timespan(self.max_waiting_time)
    def get_formatted_waiting_time(self):
        return HF.format_timespan(self.waiting_time)
    def __str__(self) -> str:
        return "Task(id={}, operation={})".format(self.id,self.operation)






# import time, uuid

# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------




# ---------- Pydantic envelopes ----------
class AxoRequestEnvelope(BaseModel):
    # transport / routing
    msg_id: str                         = Field(default_factory=lambda: _generate_id(size=AXO_ID_SIZE), description="Unique id for this request")
    task_id: Optional[str]              = Field(None, description="Optional client task id / correlation id")
    operation: str                      = Field(..., description="PUT.METADATA | METHOD.EXEC | ELASTICITY | PING | ...")
    method: Optional[str]               = None             # e.g., "sum"
    allow_stale: Optional[bool]         = False
    path:Optional[str]                  = None
    source_path:Optional[str]           = None
    sink_path:Optional[str]             = None
    axo_is_read_only:Optional[bool]     = False
    axo_key: Optional[str]              = None
    axo_module:Optional[str]            = None
    axo_class_name:Optional[str]        = None
    axo_version: Optional[int]          = None
    axo_bucket_id: Optional[str]        = None
    axo_source_bucket_id: Optional[str] = None
    axo_sink_bucket_id: Optional[str]   = None
    axo_endpoint_id: Optional[str]      = None
    axo_dependencies: List[str]         = Field(default_factory=list)
    axo_uri: Optional[str]              = None          # e.g., "axo://axo/CALC"
    axo_alias:Optional[str]             = None


    @field_validator("operation")
    @classmethod
    def _op_upper(cls, v: str) -> str:
        return v.strip().upper()
    
    @model_validator(mode="after")
    def _autofill_axo_uri(self):
        # Only fill if caller didn't set axo_uri explicitly
        if not self.axo_uri:
            self.axo_uri = _build_axo_uri(self.axo_bucket_id,self.axo_key, self.axo_class_name, self.method)
        return self
    def get_metadatax(self)->MetadataX:
        x = self
        return MetadataX(
            axo_alias            = x.axo_alias,
            axo_bucket_id        = x.axo_bucket_id,
            axo_class_name       = x.axo_class_name,
            axo_dependencies     = x.axo_dependencies,
            axo_endpoint_id      = x.axo_endpoint_id,
            axo_is_read_only     = x.axo_is_read_only,
            axo_key              = x.axo_key,
            axo_module           = x.axo_module,
            axo_sink_bucket_id   = x.axo_sink_bucket_id,
            axo_source_bucket_id = x.axo_source_bucket_id,
            axo_uri              = x.axo_uri,
            axo_version          = x.axo_version
        )
class AxoReplyEnvelope(BaseModel):
    # mirrors request + result status
    msg_id: Optional[str] = None
    task_id: Optional[str] = None
    operation: str
    status: str  # "ok" | "error"
    status_code:int

    # versioning
    pre_version: Optional[int] = None
    post_version: Optional[int] = None

    # echo
    axo_uri: Optional[str] = None
    method: Optional[str] = None
    service_time:float = 0

    # optional structured error
    error: Optional[Dict[str, Any]] = None
    warnings: List[str] = Field(default_factory=list)




class AxoRequestMsg(BaseModel):
    envelope: AxoRequestEnvelope
    payload: List[bytes] = Field(default_factory=list)  # optional extra frames

    def to_frames(self) -> List[bytes]:
        return [
            MAGIC,
            PROTO,
            self.envelope.operation.encode("utf-8"),
            JSON_CT,
            self.envelope.model_dump_json().encode("utf-8"),
            *self.payload,
        ]

    @staticmethod
    def from_frames(frames: List[bytes]) -> Result[Tuple["AxoRequestMsg", List[bytes]], Exception]:
        try:
            if len(frames) < 5:
                return Err(Exception(f"Malformed request: expected ≥5 frames, got {len(frames)}"))
            magic, proto, op_b, ctype, env_b, *payload = frames
            if magic != MAGIC:   return Err(Exception(f"Invalid magic: {magic!r}"))
            if proto != PROTO:   return Err(Exception(f"Unsupported protocol version: {proto!r}"))
            if ctype != JSON_CT: return Err(Exception(f"Unsupported content type: {ctype!r}"))
            # Parse envelope JSON (force operation from frame 2)
            env_dict = J.loads(env_b)
            env_dict["operation"] = op_b.decode("utf-8", errors="replace").strip().upper()
            env = AxoRequestEnvelope.model_validate(env_dict)
            return Ok((AxoRequestMsg(envelope=env, payload=payload), payload))
        except (J.JSONDecodeError, ValidationError, Exception) as e:
            return Err(Exception(f"Request parse error: {e}"))


class AxoReplyMsg(BaseModel):
    envelope: AxoReplyEnvelope
    payload: List[bytes] = Field(default_factory=list)

    def to_frames(self) -> List[bytes]:
        return [
            MAGIC,
            PROTO,
            self.envelope.operation.encode("utf-8"),
            JSON_CT,
            self.envelope.model_dump_json().encode("utf-8"),
            *self.payload,
        ]

    @staticmethod
    def from_frames(frames: List[bytes], expect_operation: Optional[str] = None
    ) -> Result[Tuple["AxoReplyMsg", List[bytes]], AxoError]:
        try:
            if len(frames) < 5:
                return Err(AxoError.make(msg = f"Malformed reply: expected ≥5 frames, got {len(frames)}",error_type=AxoErrorType.BAD_REQUEST ))
            magic, proto, op_b, ctype, env_b, *payload = frames
            if magic != MAGIC:   return Err(AxoError.make(msg = f"Invalid magic: {magic!r}", error_type=AxoErrorType.BAD_REQUEST))
            if proto != PROTO:   return Err(AxoError.make(msg=f"Unsupported protocol version: {proto!r}", error_type=AxoErrorType.BAD_REQUEST))
            if ctype != JSON_CT: return Err(AxoError.make(msg = f"Unsupported content type: {ctype!r}", error_type=AxoErrorType.BAD_REQUEST))
            op = op_b.decode("utf-8", errors="replace").strip().upper()

            if expect_operation and op != expect_operation.upper():
                return Err(AxoError.make(msg = f"Unexpected reply op={op}, expected={expect_operation}", error_type=AxoErrorType.BAD_REQUEST))
            env = AxoReplyEnvelope.model_validate(J.loads(env_b))
            return Ok((AxoReplyMsg(envelope=env, payload=payload), payload))
        except (J.JSONDecodeError, ValidationError, Exception) as e:
            return Err(AxoError.make(msg = f"Reply parse error: {e}", error_type=AxoErrorType.BAD_REQUEST))




class Ping(AxoRequestMsg):
    def __init__(
        self,
        *,
        axo_endpoint_id: str="",
        axo_bucket_id: str="",
        axo_key: str="",
        axo_source_bucket_id: str="",
        axo_sink_bucket_id: str="",
        dependencies: Optional[List[str]] = None,
        task_id: Optional[str] = None,
    ):
        env = AxoRequestEnvelope(
            msg_id=_generate_id(size=24),
            task_id=task_id,
            operation="PING",
            allow_stale=True,
            axo_dependencies=dependencies or [],
            axo_sink_bucket_id=axo_sink_bucket_id,
            axo_source_bucket_id=axo_source_bucket_id,
            axo_key=axo_key,
            axo_bucket_id=axo_bucket_id,
            axo_endpoint_id=axo_endpoint_id,
        )
        super().__init__(envelope=env, payload=[])  # no extra frames for PING

    @staticmethod
    def parse_pong(frames: List[bytes]) -> Result[AxoReplyEnvelope, AxoError]:
        parsed = AxoReplyMsg.from_frames(frames, expect_operation="PONG")
        if parsed.is_err:
            return Err(parsed.unwrap_err())
        msg, _ = parsed.unwrap()
        if msg.envelope.status != "ok":
            detail = (msg.envelope.error or {}).get("message", "Ping failed")
            return Err(AxoError.make(msg = detail, error_type=AxoErrorType.INTERNAL_ERROR))
        return Ok(msg.envelope)



class PutMetadata(AxoRequestMsg):
    def __init__(
        self,
        *,
        metadata:MetadataX,
        # axo_endpoint_id: str="",
        # axo_bucket_id: str="",
        # axo_key: str="",
        # axo_source_bucket_id: str="",
        # axo_sink_bucket_id: str="",
        # dependencies: Optional[List[str]] = None,
        allow_stale:bool = True,
        task_id: Optional[str] = None,
    ):
        env = AxoRequestEnvelope(
            msg_id               = _generate_id(size=AXO_ID_SIZE),
            task_id              = task_id,
            operation            = AxoOperationType.PUT_METADATA,
            allow_stale          = allow_stale,
            axo_dependencies     = metadata.axo_dependencies or [],
            axo_sink_bucket_id   = metadata.axo_sink_bucket_id,
            axo_source_bucket_id = metadata.axo_source_bucket_id,
            axo_key              = metadata.axo_key,
            axo_bucket_id        = metadata.axo_bucket_id,
            axo_endpoint_id      = metadata.axo_endpoint_id,
            axo_version          = metadata.axo_version,
            axo_uri              = metadata.axo_uri,
            axo_class_name       = metadata.axo_class_name,
            axo_alias            = metadata.axo_alias,
            axo_is_read_only     = metadata.axo_is_read_only,
            axo_module           = metadata.axo_module,
            path                 = metadata.path,
            sink_path            = metadata.sink_path,
            source_path          = metadata.source_path,
        )

        super().__init__(envelope=env, payload=[])  # no extra frames for PING


    @staticmethod
    def parse_reply(frames: List[bytes]) -> Result[AxoReplyEnvelope, AxoError]:
        parsed = AxoReplyMsg.from_frames(frames, expect_operation=AxoOperationType.PUT_METADATA)
        if parsed.is_err:
            return Err(parsed.unwrap_err())
        msg, _ = parsed.unwrap()
        if msg.envelope.status != "ok":
            detail = (msg.envelope.error or {}).get("message", "Put metadata failed")
            return Err(AxoError.make(msg = detail, error_type=AxoErrorType.INTERNAL_ERROR))
        return Ok(msg.envelope)


class MethodExecution(AxoRequestMsg):
    def __init__(
        self,
        *,
        method:str,
        metadata:MetadataX,
        fargs: list[Any] | None = [],
        fkwargs: Dict[str, Any] | None = {},
        allow_stale:bool = True,
        task_id: Optional[str] = None,
    ):
        _fargs = CP.dumps(fargs or [])
        _fkwargs = CP.dumps(fkwargs or {})
        env = AxoRequestEnvelope(
            msg_id               = _generate_id(size=AXO_ID_SIZE),
            task_id              = task_id,
            operation            = AxoOperationType.METHOD_EXEC,
            allow_stale          = allow_stale,
            axo_dependencies     = metadata.axo_dependencies or [],
            axo_sink_bucket_id   = metadata.axo_sink_bucket_id,
            axo_source_bucket_id = metadata.axo_source_bucket_id,
            axo_key              = metadata.axo_key,
            axo_bucket_id        = metadata.axo_bucket_id,
            axo_endpoint_id      = metadata.axo_endpoint_id,
            axo_version          = metadata.axo_version,
            axo_uri              = metadata.axo_uri,
            axo_class_name       = metadata.axo_class_name,
            axo_alias            = metadata.axo_alias,
            axo_is_read_only     = metadata.axo_is_read_only,
            axo_module           = metadata.axo_module,
            path                 = metadata.path,
            sink_path            = metadata.sink_path,
            source_path          = metadata.source_path,
            method = method
        )

        super().__init__(envelope=env, payload=[_fargs,_fkwargs])  # no extra frames for PING
class TaskExecution(AxoRequestMsg):
    def __init__(
        self,
        *,
        method:str,
        metadata:MetadataX,
        ctx:AxoContext,
        fargs: list[Any] | None = [],
        fkwargs: Dict[str, Any] | None = {},
        allow_stale:bool = True,
        task_id: Optional[str] = None,
    ):
        _fargs   = CP.dumps(fargs or [])
        _fkwargs = CP.dumps(fkwargs or {})
        _ctx     = CP.dumps(ctx)
        env = AxoRequestEnvelope(
            msg_id               = _generate_id(size=AXO_ID_SIZE),
            task_id              = task_id,
            operation            = AxoOperationType.TASK_EXEC,
            allow_stale          = allow_stale,
            axo_dependencies     = metadata.axo_dependencies or [],
            axo_sink_bucket_id   = metadata.axo_sink_bucket_id,
            axo_source_bucket_id = metadata.axo_source_bucket_id,
            axo_key              = metadata.axo_key,
            axo_bucket_id        = metadata.axo_bucket_id,
            axo_endpoint_id      = metadata.axo_endpoint_id,
            axo_version          = metadata.axo_version,
            axo_uri              = metadata.axo_uri,
            axo_class_name       = metadata.axo_class_name,
            axo_alias            = metadata.axo_alias,
            axo_is_read_only     = metadata.axo_is_read_only,
            axo_module           = metadata.axo_module,
            path                 = metadata.path,
            sink_path            = metadata.sink_path,
            source_path          = metadata.source_path,
            method = method
        )

        super().__init__(envelope=env, payload=[_fargs,_fkwargs,_ctx])  # no extra frames for PING

class StreamExecution(AxoRequestMsg):
    def __init__(
        self,
        *,
        method:str,
        metadata:MetadataX,
        fargs: list[Any] | None = [],
        fkwargs: Dict[str, Any] | None = {},
        allow_stale:bool = True,
        task_id: Optional[str] = None,
    ):
        _fargs = CP.dumps(fargs or [])
        _fkwargs = CP.dumps(fkwargs or {})
        env = AxoRequestEnvelope(
            msg_id               = _generate_id(size=AXO_ID_SIZE),
            task_id              = task_id,
            operation            = AxoOperationType.STREAM_EXEC,
            allow_stale          = allow_stale,
            axo_dependencies     = metadata.axo_dependencies or [],
            axo_sink_bucket_id   = metadata.axo_sink_bucket_id,
            axo_source_bucket_id = metadata.axo_source_bucket_id,
            axo_key              = metadata.axo_key,
            axo_bucket_id        = metadata.axo_bucket_id,
            axo_endpoint_id      = metadata.axo_endpoint_id,
            axo_version          = metadata.axo_version,
            axo_uri              = metadata.axo_uri,
            axo_class_name       = metadata.axo_class_name,
            axo_alias            = metadata.axo_alias,
            axo_is_read_only     = metadata.axo_is_read_only,
            axo_module           = metadata.axo_module,
            path                 = metadata.path,
            sink_path            = metadata.sink_path,
            source_path          = metadata.source_path,
            method = method
        )

        super().__init__(envelope=env, payload=[_fargs,_fkwargs])  # no extra frames for PING

    # @staticmethod
    # def parse_reply(frames: List[bytes]) -> Result[AxoReplyEnvelope, AxoError]:
    #     parsed = AxoReplyMsg.from_frames(frames, expect_operation=AxoOperationType.PUT_METADATA)
    #     if parsed.is_err:
    #         return Err(parsed.unwrap_err())
    #     msg, _ = parsed.unwrap()
    #     if msg.envelope.status != "ok":
    #         detail = (msg.envelope.error or {}).get("message", "Put metadata failed")
    #         return Err(AxoError.make(msg = detail, error_type=AxoErrorType.INTERNAL_ERROR))
    #     return Ok(msg.envelope)