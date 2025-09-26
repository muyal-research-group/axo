from abc import ABC,abstractmethod
from typing import Dict,Iterator,Optional
from option import Result
from pydantic import BaseModel,Field
from axo.errors import AxoError,AxoErrorType
from xolo.utils.utils import Utils as XoloUtils
ChunkIter   = Iterator[bytes]
ComposedKey = str
BucketId    = str
BallId      = str

class AxoStorageMetadata(BaseModel):
    key:str # Unique identifier 
    ball_id:str # Unique identifier used for segmentation purposes ball_id -> [chunk1, chunk2,...,chunkN]
    size:int # Size in bytes of the data
    checksum:str # Sha256 checksum
    producer_id:str # Unique identifier of the user that allocate the data. 
    bucket_id:str 
    tags:Optional[Dict[str,str]]={} # User-defined metadata
    content_type:Optional[str] = "application/octet-stream" # Define the type of the content
    is_disabled:Optional[bool]=Field(default=False)
    

# ---------- DTO for read results ----------

AxoObjectData = bytes 

class AxoObjectBlob:
    def __init__(self,data:AxoObjectData,metadata:AxoStorageMetadata):
        self.data     = data
        self.metadata = metadata
    
    @staticmethod
    def from_source_code(bucket_id:str,ball_id:str,key:str,code:str,producer_id:str="axo"):
        source_code_data = code.encode('utf-8') 
        code_checksum = XoloUtils.sha256(source_code_data)
        axo_blob = AxoObjectBlob(
            data=source_code_data,
            metadata=AxoStorageMetadata(
                content_type = "text/plain",
                ball_id      = f"{ball_id}_source_code",
                tags         = {
                    
                },
                bucket_id    =bucket_id,
                checksum     = code_checksum,
                is_disabled  = False,
                key          = key,
                producer_id  = producer_id,
                size         = len(source_code_data)
            )
        )        
        return axo_blob

class AxoObjectBlobs:
    """
    Raw blobs + tags; the loader (step 2) will interpret them.
    """
    def __init__(
        self,
        *,
        source_code_blob: AxoObjectBlob,
        attrs_blob:AxoObjectBlob
    ) -> None:
        self.source_code_blob = source_code_blob
        self.attrs_blob = attrs_blob



class StorageService(ABC):
    """
    Common interface for every Axo storage backend (local, S3, MictlanX, …).

    All I/O operations are asynchronous to avoid blocking the event loop.
    """

    def __init__(self, storage_service_id: str) -> None:
        self.storage_service_id = storage_service_id

    # ------------------------------------------------------------------ #
    # CRUD (bytes)
    # ------------------------------------------------------------------ #
    @abstractmethod
    async def put(
        self,
        *,
        bucket_id: str,
        key: str,
        data: bytes,
        tags: Dict[str, str] = {},
        chunk_size: str = "1MB",
    ) -> Result[str, AxoError]:
        """Upload a full blob (bytes)."""

    @abstractmethod
    async def get(
        self, *, bucket_id: str, key: str, chunk_size: str = "1MB"
    ) -> Result[bytes, AxoError]:
        """Download entire object into memory."""

    
    @abstractmethod
    async def delete(self,*,bucket_id:str,key:str)->Result[bool, AxoError]:
        ...
 
    @abstractmethod
    async def get_metadata(
        self, 
        bucket_id: str,
        key:str
    ) -> Result[AxoStorageMetadata, AxoError]:
        ...
    @abstractmethod
    async def put_data_from_file(
        self,
        *,
        source_path: str,
        key: str,
        bucket_id: str,
        tags: Dict[str, str] = {},
        chunk_size: str = "1MB",
    ) -> Result[bool, AxoError]:
        """Upload a local file directly from disk."""
