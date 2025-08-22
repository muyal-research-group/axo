import pytest
from axo.storage.data import LocalStorageService,MictlanXStorageService
from mictlanx.v4.asyncx import AsyncClient
from mictlanx.utils.index import Utils
# from mictlanx


@pytest.fixture()
def local_ss():
    return LocalStorageService(
        storage_service_id="local",
        sink_path="/sink"
    )
@pytest.fixture()
def distributed_ss():
    protocol = "http"
    return MictlanXStorageService(
        bucket_id   = "b1",
        protocol    = protocol,
        routers_str = "mictlanx-router-0:localhost:60666"
    )


@pytest.mark.asyncio
async def test_local_put(local_ss:LocalStorageService):
    bucket_id = "axo"
    key       = "k1"
    res       = await local_ss.put(bucket_id=bucket_id,key=key,data=b"HOLA")
    assert res.is_ok
@pytest.mark.asyncio
async def test_distributed_put(distributed_ss:MictlanXStorageService):
    bucket_id = "b1"
    key       = "k1"
    res       = await distributed_ss.delete(bucket_id=bucket_id,key=key)
    assert (res.is_ok or res.is_err)
    res       = await distributed_ss.put(bucket_id=bucket_id,key=key,data=b"HOLA")
    assert res.is_ok


@pytest.mark.asyncio
async def test_local_put_streaming(local_ss:LocalStorageService):
    bucket_id = "axo"
    key       = "k2"
    data = list(Utils.to_gen_bytes(b"HOLA"))

    res       = await local_ss.put_streaming(bucket_id=bucket_id,key=key,data=data)
    assert res.is_ok

@pytest.mark.asyncio
async def test_local_put_from_file(local_ss:LocalStorageService):
    bucket_id   = "axo"
    key         = "k3"
    source_path = "/sink/x.pdf"
    res       = await local_ss.put_data_from_file(source_path=source_path,bucket_id=bucket_id,key=key)
    assert res.is_ok

@pytest.mark.asyncio
async def test_local_get(local_ss:LocalStorageService):
    bucket_id = "axo"
    key       = "k1"
    res       = await local_ss.get(bucket_id=bucket_id,key=key)
    assert res.is_ok

@pytest.mark.asyncio
async def test_get_bucket_metadata(local_ss:LocalStorageService):
    bucket_id = "axo"
    key       = "k1"
    res       = await local_ss.put(bucket_id=bucket_id,key=key,data=b"HOLA")
    res       = await local_ss.get_bucket_metadata(bucket_id)
    assert res.is_ok


