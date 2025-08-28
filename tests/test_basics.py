import pytest
import pytest_asyncio
from axo import Axo,axo_method
from axo.contextmanager import AxoContextManager
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.services import MictlanXStorageService
from axo.storage.services import StorageService
from axo.storage import AxoStorage

@pytest_asyncio.fixture(scope="session", autouse=True)
# @pytest.mark.asyncio
async def before_all_tests():
    ss = MictlanXStorageService(
        bucket_id   = "b1",
        protocol    = "http",
        routers_str = "mictlanx-router-0:localhost:60666"
    )
    bids = ["axo","b1","bao","baox"]
    for bid in bids:
        res = await ss.client.delete_bucket(bid)
        print(f"BUCKET [{bid}] was clean")
    yield
class Calculator(Axo):
    from typing import List
    @axo_method
    def sum(self, xs:List[float],**kwargs):
        print(kwargs)
        return sum(xs)
    
    @axo_method
    def substract(self, xs:List[float],**kwargs)->float:
        from functools import reduce
        return reduce(lambda x,y: x-y, xs)
    @axo_method
    def divide(self, xs:List[float],**kwargs)->float:
        from functools import reduce
        try: 
            return reduce(lambda x,y: x/y,xs)
        except ZeroDivisionError as e:
            return 0.0
    
    @axo_method
    def mult(self, xs:List[float],**kwargs)->float:
        from functools import reduce
        return reduce(lambda x,y: x*y,xs)

@pytest.fixture()
def dem():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id="axo-endpoint-0",
        hostname="localhost",
        protocol="tcp",
        req_res_port=16667,
        pubsub_port=16666
    )
    return dem

@pytest.fixture
def storage_service() -> StorageService:
    return MictlanXStorageService(protocol="http")
# 
# @pytest.fixture
# def axo_storage(storage_service):
    # return AxoStorage(storage=storage_service)


# @pytest.mark.asyncio
def test_local():
    with AxoContextManager.local() as cmx:
        c:Calculator = Calculator()
        res = c.sum([0,1,2])
        print(res)


@pytest.mark.asyncio
async def test_distributed(dem:DistributedEndpointManager,storage_service:StorageService):
    with AxoContextManager.distributed( endpoint_manager= dem, storage_service=storage_service) as cmx:
        
        c:Calculator = Calculator(axo_endpoint_id = "axo-endpoint-0")
        
        res = await c.persistify()
        assert res.is_ok
        res = c.sum([0,1,2])
        print(res)
        res = c.substract([0,1,2])
        print(res)
        res = c.mult([3,2,1])
        print(res)
        res = c.divide([3,2,1])
        print(res)

@pytest.mark.asyncio
async def test_get_ao(dem:DistributedEndpointManager,storage_service:StorageService):
    with AxoContextManager.distributed( endpoint_manager= dem,storage_service=storage_service) as cmx:
        axo_key       = "akeyx"
        axo_bucket_id = "baox"
        c:Calculator = Calculator(
            axo_endpoint_id = "axo-endpoint-0",
            axo_key         = axo_key,
            axo_bucket_id   = axo_bucket_id
        )
        # print("AXO_KEY",a)
        res = await c.persistify()
        assert res.is_ok
        ao = await Axo.get_by_key(bucket_id=axo_bucket_id,key=axo_key )
        assert ao.is_ok
        calc = ao.unwrap()
        # print("CALC",calc)
        sum_res = calc.sum([1,2])
        assert sum_res.is_ok
        sum_result = sum_res.unwrap()
        assert sum_result == 3