import os
import asyncio
import pytest
from axo.storage.services import LocalStorageService
from axo.errors import AxoErrorType


@pytest.fixture
def svc(tmp_path) -> LocalStorageService:
    return LocalStorageService(sink_path=str(tmp_path / "sink"))


@pytest.fixture
def bucket() -> str:
    return "bkt"


@pytest.fixture
def key() -> str:
    return "k1"


@pytest.mark.asyncio
async def test_put_get_metadata_persisted(svc: LocalStorageService, bucket, key):
    data = b"hello"
    r = await svc.put(bucket_id=bucket, key=key, data=data)
    print(r)
    assert r.is_ok

    # bytes roundtrip
    g = await svc.get(bucket_id=bucket, key=key)
    assert g.is_ok and g.unwrap() == data

    # metadata JSON exists
    m = await svc.get_metadata(bucket, key)
    assert m.is_ok
    md = m.unwrap()
    assert md.key == key
    # check file exists on disk
    meta_path = os.path.join(svc.meta_root, bucket, f"{key}.json")
    assert os.path.exists(meta_path)

@pytest.mark.asyncio
async def test_idempotent_put_same_checksum(svc: LocalStorageService, bucket, key):
    data = b"A" * 10
    r1 = await svc.put(bucket_id=bucket, key=key, data=data)
    assert r1.is_ok
    dpath = r1.unwrap()
    t1 = os.path.getmtime(dpath)

    await asyncio.sleep(0.02)
    r2 = await svc.put(bucket_id=bucket, key=key, data=data)
    assert r2.is_ok
    t2 = os.path.getmtime(dpath)
    assert t2 == t1  # not rewritten

@pytest.mark.asyncio
async def test_get_not_found_maps_to_axoerror(svc: LocalStorageService, bucket):
    r = await svc.get(bucket_id=bucket, key="missing")
    assert r.is_err
    ax = r.unwrap_err()
    assert ax.type == AxoErrorType.GET_DATA_FAILED

@pytest.mark.asyncio
async def test_delete_cleans_data_and_metadata(svc: LocalStorageService, bucket, key):
    await svc.put(bucket_id=bucket, key=key, data=b"x")
    # ensure files exist
    dpath = os.path.join(svc.sink_path, bucket, key)
    mpath = os.path.join(svc.meta_root, bucket, f"{key}.json")
    assert os.path.exists(dpath) and os.path.exists(mpath)

    r = await svc.delete(bucket_id=bucket, key=key)
    assert r.is_ok and r.unwrap() is True
    assert not os.path.exists(dpath)
    assert not os.path.exists(mpath)
