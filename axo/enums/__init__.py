from enum import Enum
class StrEnum(str, Enum):
    """
    Backport of Python 3.11's StrEnum for Python < 3.11.
    Enum members are also instances of str.
    """

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

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