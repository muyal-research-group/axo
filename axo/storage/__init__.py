from axo.storage.services import StorageService
from option import Result,Ok,Err
from axo.errors import AxoError,AxoErrorType
from axo.storage.types import (
    StorageService,
    AxoStorageMetadata,
    AxoObjectBlobs,
    AxoObjectBlob,
    BucketId,
    ComposedKey,
)
from axo.storage.constants import (
    AXO_SOURCE_CODE_SUFFIX,
    AXO_ATTRS_SUFFIX,
    AXO_SCHEMA_VERSION,
)
from axo.storage.utils import StorageUtils as SU

class AxoStorage:
    """
    Knows how to name and tag the two pieces of an Axo object:
        <key>_{AXO_SOURCE_CODE_SUFFIX}, <key>_{AXO_ATTRS_SUFFIX}
    but remains Axo-agnostic (no imports from Axo Core; no exec).
    """

    def __init__(self, storage: StorageService) -> None:
        self.storage = storage
    # ----- keys -----


    # -------------------- WRITE (from blobs) --------------------

    async def put_blobs(
        self,
        *,
        bucket_id: BucketId,
        key: ComposedKey,
        blobs: AxoObjectBlobs,
        class_name: str,
        chunk_size: str = "1MB",
    ) -> Result[bool, AxoError]:
        """
        Accepts two AxoObjectBlob instances (source_code + attrs) plus class identity.
        Validates metadata vs bytes; writes with metadata-derived tags + Axo tags.
        """

        expected_src_key  = SU.source_key(key)
        expected_attr_key = SU.attrs_key(key)

        # enforce naming convention
        if blobs.source_code_blob.metadata.key != expected_src_key:
            return Err(
                AxoError.make(
                    error_type=AxoErrorType.STORAGE_ERROR,
                    msg=f"source key mismatch: {blobs.source_code_blob.metadata.key} != {expected_src_key}"
                )
            )
        if blobs.attrs_blob.metadata.key != expected_attr_key:
            return Err(
                AxoError.make(
                    error_type=AxoErrorType.STORAGE_ERROR,
                    msg = f"attrs key mismatch: {blobs.attrs_blob.metadata.key} != {expected_attr_key}"
                )
            )

        # integrity validations
        err = SU._validate_blob_integrity(blobs.source_code_blob)
        if err:
            return Err(
                AxoError.make(
                    error_type=AxoErrorType.VALIDATION_FAILED,
                    msg = f"Validation Error: {err}"
                )
            )
        
        err = SU._validate_blob_integrity(blobs.attrs_blob)
        if err:
            return Err(
                AxoError.make(
                    error_type=AxoErrorType.VALIDATION_FAILED,
                    msg=f"Validation Erro: {err}"
                )
            )

        # Axo-specific extra tags
        src_extra = {
            "axo_source_code_suffix": AXO_SOURCE_CODE_SUFFIX,
            "axo_schema_ver": AXO_SCHEMA_VERSION,
            "axo_class_name": class_name,
        }
        attr_extra = {
            "axo_attrs_suffix": AXO_ATTRS_SUFFIX,
            "axo_schema_ver": AXO_SCHEMA_VERSION,
            "axo_class_name": class_name,
        }
        # Flatten to tags for the backend
        src_tags  = SU.to_tags(blobs.source_code_blob.metadata, src_extra)
        attr_tags = SU.to_tags(blobs.attrs_blob.metadata,    attr_extra)

        # 1) put source
        r1 = await self.storage.put(
            bucket_id  = bucket_id,
            key        = expected_src_key,
            data       = blobs.source_code_blob.data,
            tags       = src_tags,
            chunk_size = chunk_size,
        )
        if r1.is_err:
            return Err(
                AxoError.make(
                    error_type = AxoErrorType.STORAGE_ERROR,
                    msg        = f"Failed to put {expected_src_key}"
                )
            )

        # 2) put attrs 
        r2 = await self.storage.put(
            bucket_id  = bucket_id,
            key        = expected_attr_key,
            data       = blobs.attrs_blob.data,
            tags       = attr_tags,
            chunk_size = chunk_size,
        )
        if r2.is_err:
                res = await self.storage.delete(bucket_id=bucket_id, key=expected_src_key)
                if res.is_err:
                    return Err(Exception(f"Failed to put {expected_attr_key}"))

        return Ok(True)

    # -------------------- READ (to blobs) --------------------

    async def get_blobs(
        self, *, bucket_id: BucketId, key: ComposedKey, chunk_size: str = "1MB"
    ) -> Result[AxoObjectBlobs, AxoError]:
        """
        Loads both parts and reconstructs AxoObjectBlobs. Validates integrity.
        """
        k_src  = SU.source_key(key)
        k_attr = SU.attrs_key(key)

        src_res   = await self.storage.get(bucket_id=bucket_id, key=k_src,  chunk_size=chunk_size)
        attrs_res = await self.storage.get(bucket_id=bucket_id, key=k_attr, chunk_size=chunk_size)
        if src_res.is_err:
            return Err(src_res.unwrap_err())
        if attrs_res.is_err:
            return Err(attrs_res.unwrap_err())

        
   # fetch metadata per key (your new API)
        src_meta_res  = await self.storage.get_metadata(bucket_id=bucket_id, key=k_src)
        attr_meta_res = await self.storage.get_metadata(bucket_id=bucket_id, key=k_attr)
        if src_meta_res.is_err:
            return Err(src_meta_res.unwrap_err())
        if attr_meta_res.is_err:
            return Err(attr_meta_res.unwrap_err())

        src_blob  = AxoObjectBlob(data=src_res.unwrap(),   metadata=src_meta_res.unwrap())
        attr_blob = AxoObjectBlob(data=attrs_res.unwrap(), metadata=attr_meta_res.unwrap())

        # integrity validations
        err = SU._validate_blob_integrity(src_blob)
        if err:
            return Err(
                AxoError.make(
                    error_type=AxoErrorType.VALIDATION_FAILED, 
                    msg=f"Validation error: {err}"
                )
            )
        err = SU._validate_blob_integrity(attr_blob)
        if err:
            return Err(
                AxoError.make(
                    error_type=AxoErrorType.VALIDATION_FAILED, 
                    msg=f"Validation error: {err}"
                )
            )
        return Ok(AxoObjectBlobs(source_code_blob=src_blob, attrs_blob=attr_blob))

    # -------------------- DELETE --------------------

    async def delete_object(self, *, bucket_id: BucketId, key: ComposedKey) -> Result[bool, AxoError]:
        k_src  = SU.source_key(key)
        k_attr = SU.attrs_key(key)
        e1 = await self.storage.delete(bucket_id=bucket_id, key=k_src)
        e2 = await self.storage.delete(bucket_id=bucket_id, key=k_attr)
        if e1.is_err:
            return Err(e1.unwrap_err())
        elif e2.is_err:
            return Err(e2.unwrap_err())
        else:
            return Ok(True)