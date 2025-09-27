import pytest
# 
from axo.storage import AxoStorage
from axo.storage.types import StorageService,AxoObjectBlobs,AxoStorageMetadata,AxoObjectBlob
from axo.storage.services import LocalStorageService
from axo.storage.utils import StorageUtils as SU

@pytest.fixture
def bucket_id():
    return "test-bucket"


@pytest.fixture
def base_key():
    return "obj-123"

@pytest.fixture
def class_name():
    return "MyAxoClass"

@pytest.fixture
def storage_service() -> StorageService:
    return LocalStorageService()

@pytest.fixture
def axo_storage(storage_service):
    return AxoStorage(storage=storage_service)


def make_blobs(base_key: str) -> AxoObjectBlobs:
    # fabricate code + attrs payloads
    src_key = SU.source_key(base_key)
    attr_key = SU.attrs_key(base_key)

    src_bytes = b"print('hello axo')\nclass X: pass\n"
    attr_bytes = b'{"param": 42, "name": "X"}'

    src_md = AxoStorageMetadata(
        key          = src_key,
        ball_id      = src_key,
        size         = len(src_bytes),
        checksum     = SU.sha256_hex(src_bytes),
        producer_id  = "pytest",
        bucket_id    = "test-bucket",
        tags         = {"purpose": "source"},
        content_type = "application/python",
        is_disabled  = False,
    )
    attr_md = AxoStorageMetadata(
        key          = attr_key,
        ball_id      = attr_key,
        size         = len(attr_bytes),
        checksum     = SU.sha256_hex(attr_bytes),
        producer_id  = "pytest",
        bucket_id    = "test-bucket",
        tags         = {"purpose": "attrs"},
        content_type = "application/json",
        is_disabled  = False,
    )

    return AxoObjectBlobs(
        source_code_blob = AxoObjectBlob(src_bytes, src_md),
        attrs_blob       = AxoObjectBlob(attr_bytes, attr_md),
    )


# ---------- Tests ----------

@pytest.mark.asyncio
async def test_put_get_delete_roundtrip(axo_storage: AxoStorage, bucket_id, base_key, class_name):
    blobs = make_blobs(base_key)

    # PUT
    res_put = await axo_storage.put_blobs(
        bucket_id=bucket_id, key=base_key, blobs=blobs, class_name=class_name
    )
    assert res_put.is_ok, f"put_blobs failed: {res_put.unwrap_err() if res_put.is_err else ''}"

    # GET
    res_get = await axo_storage.get_blobs(bucket_id=bucket_id, key=base_key)
    assert res_get.is_ok, f"get_blobs failed: {res_get.unwrap_err() if res_get.is_err else ''}"
    got = res_get.unwrap()
    print("got",got.source_code_blob.data)

    # verify the data and metadata match exactly
    assert got.source_code_blob.data == blobs.source_code_blob.data
    assert got.attrs_blob.data == blobs.attrs_blob.data
    assert got.source_code_blob.metadata.checksum == blobs.source_code_blob.metadata.checksum
    assert got.attrs_blob.metadata.size == blobs.attrs_blob.metadata.size

    # DELETE (both parts)
    res_del = await axo_storage.delete_object(bucket_id=bucket_id, key=base_key)
    assert res_del.is_ok and res_del.unwrap() is True
@pytest.mark.asyncio
async def test_integrity_validation_fails_on_checksum_mismatch(axo_storage: AxoStorage, bucket_id, base_key, class_name):
    blobs = make_blobs(base_key)
    # corrupt checksum on source blob
    blobs.source_code_blob.metadata.checksum = "deadbeef" * 8  # wrong checksum

    res_put = await axo_storage.put_blobs(
        bucket_id=bucket_id, key=base_key, blobs=blobs, class_name=class_name
    )
    assert res_put.is_err, "put_blobs should fail with checksum mismatch"

@pytest.mark.asyncio
async def test_key_naming_enforced(axo_storage: AxoStorage, bucket_id, base_key, class_name):
    blobs = make_blobs(base_key)
    # break the enforced naming convention
    blobs.source_code_blob.metadata.key = "wrong_name.py"

    res_put = await axo_storage.put_blobs(
        bucket_id=bucket_id, key=base_key, blobs=blobs, class_name=class_name
    )
    assert res_put.is_err, "put_blobs should fail when keys do not use the required suffixes"



@pytest.mark.asyncio
async def test_get_metadata_from_backend(axo_storage: AxoStorage, storage_service: LocalStorageService, bucket_id, base_key, class_name):
    blobs = make_blobs(base_key)
    res_put = await axo_storage.put_blobs(
        bucket_id=bucket_id, key=base_key, blobs=blobs, class_name=class_name
    )
    assert res_put.is_ok

    # Use backend metadata API directly
    src_key = SU.source_key(base_key)
    md_res = await storage_service.get_metadata(bucket_id, src_key)
    assert md_res.is_ok
    md = md_res.unwrap()
    assert md.key == src_key
    assert int(md.size) == len(blobs.source_code_blob.data)