# tests/test_refs_and_pointer.py
import json
import os
import hashlib as H
import pytest

from option import Ok, Err  # from the 'option' library

# Import your models / code under test
from axo.core.models import ChunkRef, BallRef,AxoPointer
# from axo.materialize import AxoPointer
from axo.storage.services import InMemoryStorageService
from axo.storage.types import AxoStorageMetadata
from axo.errors import AxoErrorType
from axo.storage.utils import StorageUtils as SU
from pydantic import ValidationError


# ---------- Fixtures ----------

@pytest.fixture
def storage() -> InMemoryStorageService:
    return InMemoryStorageService()

@pytest.fixture
def bucket_id() -> str:
    return "test-bucket"

@pytest.fixture
def key() -> str:
    return "obj-1"

@pytest.fixture
def payload() -> bytes:
    return b"hello axo!"

@pytest.fixture
def md(bucket_id, key, payload) -> AxoStorageMetadata:
    return AxoStorageMetadata(
        key=key,
        ball_id=key,
        size=len(payload),
        checksum=SU.sha256_hex(payload),
        producer_id="pytest",
        bucket_id=bucket_id,
        tags={"purpose": "unit"},
        content_type="application/octet-stream",
        is_disabled=False,
    )


# ---------- ChunkRef tests ----------

def test_chunkref_basic_hex_ok():
    c = ChunkRef(index=0, size=10, checksum="deadbeef" * 8)  # 64 hex chars
    assert c.index == 0
    assert c.size == 10
    assert len(c.checksum) == 64

def test_chunkref_non_hex_is_trimmed():
    c = ChunkRef(index=1, size=0, checksum="  not-hex-OK  ")
    assert c.checksum == "not-hex-OK"  # validator trims whitespace

def test_chunkref_frozen_attr_immutable():
    c = ChunkRef(index=0, size=1, checksum="a")
    with pytest.raises(ValidationError):
        c.size = 2  # pydantic frozen models forbid reassignment

    with pytest.raises(ValidationError):
        c.tags = {}  # full attribute reassignment also forbidden


# ---------- BallRef tests ----------

def test_ballref_from_metadata_single_chunk(md: AxoStorageMetadata):
    ref = BallRef.from_metadata(md)
    assert ref.v == 1
    assert ref.bucket_id == md.bucket_id
    assert ref.key == md.key
    assert ref.ball_id == md.key
    assert ref.size == md.size
    assert ref.checksum == md.checksum
    assert len(ref.chunks) == 1
    c0 = ref.chunks[0]
    assert c0.index == 0 and c0.size == md.size and c0.checksum == md.checksum
    # immutability
    with pytest.raises(ValidationError):
        ref.size = 0

def test_ballref_to_from_json_roundtrip(md: AxoStorageMetadata):
    ref1 = BallRef.from_metadata(md)
    s = ref1.to_json()
    # Ensure valid JSON and has some expected fields
    data = json.loads(s)
    assert data["bucket_id"] == md.bucket_id
    assert data["v"] == 1
    ref2 = BallRef.from_json(s)
    assert ref2 == ref1




# ---------- AxoPointer tests (real InMemoryStorageService) ----------

@pytest.mark.asyncio
async def test_pointer_into_bytes_and_consume(storage, bucket_id, key, payload):
    # Put bytes into the in-memory backend
    put_res = await storage.put(bucket_id=bucket_id, key=key, data=payload, tags={"test": "1"})
    assert put_res.is_ok

    # Build BallRef from backend metadata
    md_res = await storage.get_metadata(bucket_id, key)
    assert md_res.is_ok
    ref = BallRef.from_metadata(md_res.unwrap())

    # Create pointer with consume=True and delete_remote=False
    ptr = AxoPointer(storage, ref, consume=True, delete_remote=False)

    # First materialization works
    r1 = await ptr.into_bytes()
    assert r1.is_ok and r1.unwrap() == payload
    assert ptr.is_alive() is False  # consumed → invalidated

    # Second call fails due to consume=True
    r2 = await ptr.into_bytes()
    assert r2.is_err
    err = r2.unwrap_err()
    assert err.type == AxoErrorType.BAD_REQUEST

@pytest.mark.asyncio
async def test_pointer_as_memoryview(storage, bucket_id, key, payload):
    # Put and build ref
    put_res = await storage.put(bucket_id=bucket_id, key=key, data=payload, tags={})
    assert put_res.is_ok
    md = (await storage.get_metadata(bucket_id, key)).unwrap()
    ref = BallRef.from_metadata(md)

    ptr = AxoPointer(storage, ref, consume=False, delete_remote=False)
    mv_res = await ptr.as_memoryview()
    assert mv_res.is_ok
    mv = mv_res.unwrap()
    assert isinstance(mv, memoryview)
    assert bytes(mv) == payload
    assert ptr.is_alive() is True  # not consumed

@pytest.mark.asyncio
async def test_pointer_into_file_and_overwrite_flags(tmp_path, storage, bucket_id, key, payload):
    # Put and build ref
    await storage.put(bucket_id=bucket_id, key=key, data=payload, tags={})
    ref = BallRef.from_metadata((await storage.get_metadata(bucket_id, key)).unwrap())

    file_path = tmp_path / "out.bin"
    ptr = AxoPointer(storage, ref, consume=False, delete_remote=False)

    # First write
    r1 = await ptr.into_file(str(file_path), overwrite=True)
    assert r1.is_ok and os.path.exists(r1.unwrap())
    with open(file_path, "rb") as fh:
        assert fh.read() == payload

    # No-overwrite should fail
    r2 = await ptr.into_file(str(file_path), overwrite=False)
    assert r2.is_err
    assert r2.unwrap_err().type == AxoErrorType.ALREADY_EXISTS

@pytest.mark.asyncio
async def test_pointer_delete_remote(storage, bucket_id, key, payload):
    # Put and build ref
    await storage.put(bucket_id=bucket_id, key=key, data=payload, tags={})
    ref = BallRef.from_metadata((await storage.get_metadata(bucket_id, key)).unwrap())

    # delete_remote=True should remove the object after materialization
    ptr = AxoPointer(storage, ref, consume=False, delete_remote=True)
    r = await ptr.into_bytes()
    assert r.is_ok and r.unwrap() == payload

    # Now the backend should not find it
    get_res = await storage.get(bucket_id=bucket_id, key=key)
    assert get_res.is_err

@pytest.mark.asyncio
async def test_pointer_large_memoryview_guard(storage, bucket_id, key):
    # Create a payload larger than the guard
    big = b"x" * (1 * 1024 * 1024 + 1)  # 256MiB + 1
    await storage.put(bucket_id=bucket_id, key=key, data=big, tags={})
    ref = BallRef.from_metadata((await storage.get_metadata(bucket_id, key)).unwrap())

    ptr = AxoPointer(storage, ref, consume=False, delete_remote=False)
    mv_res = await ptr.as_memoryview(max_bytes="256kb")  # default max_bytes 256kb
    assert mv_res.is_err
    assert mv_res.unwrap_err().type == AxoErrorType.VALIDATION_FAILED
