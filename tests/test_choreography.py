import pytest
import pytest_asyncio
from axo import axo_method,Axo,axo_task,axo_stream
from axo.contextmanager import AxoContextManager
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.services import MictlanXStorageService,StorageService

class GrayScaler(Axo):
    @axo_task()
    def to_grayscale(**kwargs)->bytes:
        from PIL import Image
        import io
        
        image_bytes:bytes = kwargs.get("source",None)
        if image_bytes:
            img = Image.open(io.BytesIO(image_bytes))
            # Convert to grayscale
            gray = img.convert("L")
            buf = io.BytesIO()
            gray.save(buf, format="PNG")
            return buf
        raise Exception("No source was provided")

            # return buf.getvalue()
    
class Compresser(Axo):
    from axo.core.models import AxoContext
    @axo_task(source_bucket="e3uiviu4s3jmiuw6",sink_bucket="sinkbucket")
    def zip(
        self,
        source: bytes,
        name:str = "payload",
        *,
        ctx: AxoContext = AxoContext(),
    ) -> bytes:
        """
        Create a ZIP archive entirely in memory.
        - If `source` is raw bytes, stores it under `name`.
        - If `source` is a mapping, each key is a filename and each value its bytes.
        Returns the ZIP file as bytes.
        """
        import io
        import zipfile
        print("CTX",ctx)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(name, source)

        return buf.getvalue()
    def unzip(
        self,
        archive: bytes,
        name:str = "payload",
        *,
        ctx: AxoContext = AxoContext(),
    ) -> bytes:
        """
        Extract a single file from an in-memory ZIP and return its bytes.
        - If `name` is provided, that member is returned.
        - Otherwise the first member in the archive is returned.
        Raises ValueError if the archive is empty or `name` is not found.
        """
        import io
        import zipfile

        with zipfile.ZipFile(io.BytesIO(archive), "r") as zf:
            names = zf.namelist()
            if not names:
                raise ValueError("empty ZIP archive")
            target = name if name is not None else names[0]
            if target not in names:
                raise ValueError(f"'{target}' not found in ZIP (members: {names})")
            return zf.read(target)



@pytest_asyncio.fixture(scope="session", autouse=True)
# @pytest.mark.asyncio
async def before_all_tests():
    ss = MictlanXStorageService(
        bucket_id   = "b1",
    )
    bids = ["bsg-0","bcp-0","sinkbucket"]
    for bid in bids:
        res = await ss.client.delete_bucket(bid)
        print(f"BUCKET [{bid}] was clean")
    yield

@pytest.fixture()
def endpoint_manager():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id  = "axo-endpoint-0",
        hostname     = "localhost",
        protocol     = "tcp",
        req_res_port = 16667,
        pubsub_port  = 16666
    )
    return dem

@pytest.fixture
def storage_service() -> StorageService:
    return MictlanXStorageService()

@pytest.mark.asyncio
async def test_pipeline(endpoint_manager:DistributedEndpointManager, storage_service:StorageService):

    with AxoContextManager.distributed(endpoint_manager=endpoint_manager,storage_service=storage_service) as rt:
        # 1) Get and create the instances 
        gs:GrayScaler = GrayScaler(axo_bucket_id="bgs-0") # or Axo.get_by_key(...)
        c:Compresser = Compresser(axo_bucket_id="bcp-0", axo_key="cp-0") # or Axo.get_by_key(...)
        
        
        assert (await c.persistify()).is_ok
        res = c.zip(source=b"HOLAAAAAAAAAAAAAAAAAAA xD", name="payload")
        print(res)
        # res = c.unzip(archive=res)
        # print(res)
        # 2) Store the objects
        # assert (await c.persistify()).is_ok
        # gs.to_grayscale(

        # )
        # 3) 



    

