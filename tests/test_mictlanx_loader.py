import pytest
import pytest_asyncio
from axo.storage import AxoStorage
from axo.storage.types import AxoObjectBlobs,AxoStorageMetadata,AxoObjectBlob
from axo.storage.services import MictlanXStorageService 
from axo.errors import AxoErrorType
from axo.storage.loader import AxoLoader  
from axo.storage.utils import StorageUtils as SU

@pytest.fixture(scope="session")
def bucket_id(): return "test-bucket"
@pytest.fixture(scope="session")
def base_key():  return "obj-mictlanx"
@pytest.fixture(scope="session")
def class_name(): return "Greeter"

@pytest_asyncio.fixture(scope="session", autouse=True)
# @pytest.mark.asyncio
async def before_all_tests(bucket_id:str):
    ss = MictlanXStorageService(
        bucket_id   = "b1",
        protocol    = "http",
        routers_str = "mictlanx-router-0:localhost:60666"
    )
    # bids = ["axo","b1","bao","baox"]
    # for bid in bids:
    res = await ss.client.delete_bucket(bucket_id=bucket_id)
    print(f"BUCKET [{bucket_id}] was clean")
    yield


def make_blobs_for_class(base_key: str, bucket_id: str, class_name: str, *, greeting="hola") -> AxoObjectBlobs:
    # Source defines a class that consumes "greeting" from attrs
    source = f"""
    class {class_name}:
        def __init__(self, greeting: str,**kwargs):
            self.greeting = greeting
        def greet(self, name: str) -> str:
            return f"{{self.greeting}}, {{name}}"
        def sum(self,x:int,y:int)->int:
            return x+y
    """.lstrip().encode("utf-8")

    attrs_json = f'{{"greeting": "{greeting}"}}'.encode("utf-8")

    src_key = SU.source_key(base_key)
    attr_key = SU.attrs_key(base_key)

    src_md = AxoStorageMetadata(
        key          = src_key,
        ball_id      = src_key,
        size         = len(source),
        checksum     = SU.sha256_hex(source),
        producer_id  = "pytest",
        bucket_id    = bucket_id,
        tags         = {"purpose": "source"},
        content_type = "application/python",
        is_disabled  = False,
    )
    attr_md = AxoStorageMetadata(
        key          = attr_key,
        ball_id      = attr_key,
        size         = len(attrs_json),
        checksum     = SU.sha256_hex(attrs_json),
        producer_id  = "pytest",
        bucket_id    = bucket_id,
        tags         = {"purpose": "attrs"},
        content_type = "application/json",
        is_disabled  = False,
    )
    return AxoObjectBlobs(
        source_code_blob = AxoObjectBlob(source, src_md),
        attrs_blob       = AxoObjectBlob(attrs_json, attr_md),
    )

@pytest.fixture
def storage_service() -> MictlanXStorageService:
    return MictlanXStorageService(protocol="http")

@pytest.fixture
def axo_storage(storage_service):
    return AxoStorage(storage=storage_service)

@pytest.fixture
def loader(axo_storage) -> AxoLoader:
    return AxoLoader(axo_storage)

@pytest.mark.asyncio
async def test_loader_roundtrip_mictlanx(loader: AxoLoader, axo_storage: AxoStorage, bucket_id, base_key, class_name):
    expected_greet_return = "hola, Nacho"
    expected_sum_return   = 4
    blobs = make_blobs_for_class(base_key, bucket_id, class_name, greeting="hola")
    # put with Axo tags
    res_put = await axo_storage.put_blobs(bucket_id=bucket_id, key=base_key, blobs=blobs, class_name=class_name)
    assert res_put.is_ok
    
    # load instance
    res = await loader.load_object(bucket_id=bucket_id, key=base_key)
    assert res.is_ok
    obj = res.unwrap()
    assert obj.greet("Nacho") == expected_greet_return
    assert obj.sum(1,3) == expected_sum_return

@pytest.mark.asyncio
async def test_loader_missing_class_tag(loader: AxoLoader, axo_storage: AxoStorage, bucket_id, base_key, class_name):
    # Write blobs *without* using AxoStorage.put_blobs (so we miss the axo_class_name tag)
    # We'll directly use backend to simulate a mis-tagged object.
    blobs = make_blobs_for_class(base_key, bucket_id, class_name)
    # store raw without tags → construct minimal tags set
    ss = axo_storage.storage
    await ss.put(bucket_id=bucket_id, key=SU.source_key(base_key), data=blobs.source_code_blob.data, tags={}, chunk_size="1MB")
    await ss.put(bucket_id=bucket_id, key=SU.attrs_key(base_key), data=blobs.attrs_blob.data, tags={}, chunk_size="1MB")

    res = await loader.load_object(bucket_id=bucket_id, key=base_key)
    assert res.is_err
    assert res.unwrap_err().type == AxoErrorType.VALIDATION_FAILED
