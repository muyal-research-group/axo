import os
import io
import functools
import asyncio
import pytest
import pytest_asyncio

from axo.errors import AxoErrorType
from axo.storage.types import AxoStorageMetadata
from axo.storage.services import MictlanXStorageService
from mictlanx import AsyncClient
from mictlanx.services import AsyncRouter
from mictlanx.utils.uri import MictlanXURI
# from mictlanx.v4.interfaces import AsyncRouter

import uuid

# -------------------------------- Fixtures -------------------------------- #
@pytest_asyncio.fixture(scope="session", autouse=True)
# @pytest.mark.asyncio
async def before_all_tests():
    ss = MictlanXStorageService(
        bucket_id   = "b1",
        uri = "mictlanx://mictlanx-router-0@localhost:60666?/api_version=4&protocol=http"
    )
    bids = ["axo","b1","bao","baox","bkt"]
    for bid in bids:
        res = await ss.client.delete_bucket(bid)
        print(f"BUCKET [{bid}] was clean")
    yield


@pytest.fixture
def bucket():
    return "bkt"


@pytest.fixture
def key():
    return "k1"


@pytest.fixture
def data():
    return b"hello-mictlanx"


@pytest.fixture
def svc_ok():
    """Service with a happy-path fake client."""
    routers = [
        AsyncRouter(
            router_id = "mictlanx-router-0",
            http2     = False,
            ip_addr   = "localhost",
            port      = 60666,
            protocol  = "http"
        )
    ]
    uri = MictlanXURI.build(routers=routers)

    fake = AsyncClient(
        capacity_storage = "10GB",
        client_id        = "pytest-ss",
        debug            = True,
        eviction_policy  = "LRU",
        max_workers      = 2,
        uri = uri,
    )
    s = MictlanXStorageService(client=fake)
    return s


@pytest.mark.asyncio
async def test_put_get_delete_roundtrip(
    svc_ok: MictlanXStorageService, bucket: str, key: str
):
    data = b"hello-mictlanx-e2e"

    # PUT
    r_put = await svc_ok.put(bucket_id=bucket, key=key, data=data, tags={"purpose": "e2e"})
    assert r_put.is_ok, f"put failed: {r_put.unwrap_err() if r_put.is_err else ''}"
    assert r_put.unwrap() == key
    r_get = await svc_ok.get(bucket_id=bucket, key=key)
    assert r_get.is_ok, f"get failed: {r_get.unwrap_err() if r_get.is_err else ''}"
    assert r_get.unwrap() == data
    r_get_metadata = await svc_ok.get_metadata(bucket_id=bucket, key=key)
    assert r_get_metadata.is_ok, f"get metadata failed: {r_get.unwrap_err() if r_get.is_err else ''}"
    m = r_get_metadata.unwrap()
    assert m.key == key and bucket == m.bucket_id and len(data) == m.size
    r_del = await svc_ok.delete(bucket_id=bucket, key=key)
    assert r_del.is_ok and r_del.unwrap() is True



@pytest.mark.asyncio
async def test_get_nonexistent_maps_to_not_found(
    svc_ok: MictlanXStorageService, bucket: str
):
    missing_key = f"missing-{uuid.uuid4().hex}"
    r = await svc_ok.get(bucket_id=bucket, key=missing_key)
    assert r.is_err, "expected an error for nonexistent object"
    ax = r.unwrap_err()
    assert ax.type in {AxoErrorType.NOT_FOUND ,AxoErrorType.GET_DATA_FAILED}


@pytest.mark.asyncio
async def test_delete_nonexistent_returns_error(
    svc_ok: MictlanXStorageService, bucket: str
):
    missing_key = f"missing-{uuid.uuid4().hex}"
    r = await svc_ok.delete(bucket_id=bucket, key=missing_key)
    assert r.is_err, "expected an error when deleting a nonexistent object"
    assert r.unwrap_err().type in {AxoErrorType.NOT_FOUND, AxoErrorType.DELETE_FAILED}