from enum import Enum,IntEnum,auto
class StrEnum(str, Enum):
    """
    Backport of Python 3.11's StrEnum for Python < 3.11.
    Enum members are also instances of str.
    """

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"
class TaskStatus(Enum):
    PENDING   = auto()
    RUNNING   = auto()
    SUCCESS   = auto()
    FAILED    = auto()
    TIMEOUT   = auto()
    CANCELLED = auto()

class AxoErrorType(StrEnum):
    DELETE_FAILED           = "DELETE_FAILED"

    PUT_DATA_FAILED         = "PUT_DATA_FAILED"
    PUT_METADATA_FAILED     = "PUT_METADATA_FAILED"

    GET_DATA_FAILED         = "GET_DATA_FAILED"
    GET_METADATA_FAILED     = "GET_METADATA_FAILED"

    BAD_REQUEST             = "BAD_REQUEST"
    UNKNOWN_OPERATION       = "UNKNOWN_OPERATION"
    VALIDATION_FAILED       = "VALIDATION_FAILED"
    CONCURRENCY_CONFLICT    = "CONCURRENCY_CONFLICT"
    DEPENDENCY_INSTALL_FAIL = "DEPENDENCY_INSTALL_FAIL"
    ENDPOINT_COLD           = "ENDPOINT_COLD"
    NOT_LEADER              = "NOT_LEADER"
    STORAGE_ERROR           = "STORAGE_ERROR"
    INTERNAL_ERROR          = "INTERNAL_ERROR"
    TRANSPORT_ERROR         = "TRANSPORT_ERROR"
    # 
    TIMEOUT                 = "TIMEOUT"
    NOT_FOUND               = "NOT_FOUND"
    ALREADY_EXISTS          = "ALREADY_EXISTS"

class AxoErrorCode(IntEnum):
    OK                   = 0
    DELETE_FAILED        = -399
    BAD_REQUEST          = -400
    UNKNOWN_OPERATION    = -404
    VALIDATION_FAILED    = -422
    NOT_FOUND            = -440
    ALREADY_EXISTS       = -441
    CONCURRENCY_CONFLICT = -460
    TIMEOUT              = -480
    
    STORAGE_ERROR        = -500
    PUT_DATA_FAILED   = -501
    PUT_METADATA_FAILED  = -502
    GET_DATA_FAILED   = -503
    GET_METADATA_FAILED  = -504

    DEP_INSTALL_FAIL     = -510
    ENDPOINT_COLD        = -520
    NOT_LEADER           = -530
    TRANSPORT_ERROR      = -540
    INTERNAL_ERROR       = -599
    

class AxoOperationType(StrEnum):
    PUT_METADATA    = "PUT_METADATA"
    PING            = "PING"
    METHOD_EXEC     = "METHOD_EXEC"
    UNKNOWN         = "UNKNOWN"
    CREATE_ENDPOINT = "CREATE_ENDPOINT"
    @classmethod
    def from_str(cls, value: str, default=None):
        """
        Create an enum member from a string.
        
        Args:
            value (str): Input string.
            default (Any): Value to return if not found. If None, raises ValueError.
        
        Returns:
            Color: Matching enum member.
        """
        if value is None:
            return default if default is not None else cls("UNKNOWN")
        try:
            # Direct exact match
            return cls(value.upper())
        except ValueError:
            return default if default is not None  else cls("UNKNOWN")