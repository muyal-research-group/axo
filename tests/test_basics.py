import pytest
import pytest_asyncio
from axo import Axo,axo_method,axo_task
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
    )
    bids = ["axo","b1","bao","baox"]
    for bid in bids:
        res = await ss.client.delete_bucket(bid)
        print(f"BUCKET [{bid}] was clean")
    yield





class Calculator(Axo):

    from typing import List
    # @axo_method, @axo_task @axo_stream(in working progress)
    @axo_task(source_bucket="b1", sink_bucket="b1")
    def otro_ejemplo(self):
        ...


    @axo_method
    def sum(self, xs:List[float],**kwargs)->float:
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
# 
# @pytest.fixture
# def axo_storage(storage_service):
    # return AxoStorage(storage=storage_service)


@pytest.mark.asyncio
async def test_local():
    with AxoContextManager.local() as cmx:
        
        c:Calculator = Calculator()
        ao_result = await Axo.get_by_key(bucket_id="BUCKET_ID",key="ao1")
        
        assert ao_result.is_ok
        coreography_alias = "Calculator.sum"
        coreography_params = {
            "x":2,
            "y":3
        }

        (class_name, method_name) = coreography_alias.split(".")
        ao     = ao_result.unwrap()
        ao.set_source_bucket_id("B1")
        ao.set_sink_bucket_id("B2")

        method = getattr(ao, method_name)
        parametros_del_siguiente_ao = method(**coreography_params)

        ao2_result  = await Axo.get_by_key(bucket_id="CUBET2ON",key="ao2")

        assert ao2_result.is_ok
        ao2         = ao2_result.unwrap()
        ao2.set_source_bucket_id("BUCKET_GRAPH")
        ao2.set_source_bucket_id("BUCKET_GRAPH_SALIDA")

        method2     = getattr(ao2, "multiply")
        final_result = method2(parametros_del_siguiente_ao)


        c.set_source_bucket_id("B1")
        c.set_sink_bucket_id("B2")

        res = c.sum([0,1,2],axo_endpoint_id = "axo1111")
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

@pytest.mark.skip(reason="Needs Axo Endpoint v0.0.4")
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

        ao = await Axo.get_by_key(
            bucket_id=axo_bucket_id,
            key=axo_key 
        )

        assert ao.is_ok
        calc = ao.unwrap()
        # print("CALC",calc)
        sum_res = calc.sum([1,2])
        assert sum_res.is_ok
        sum_result = sum_res.unwrap()
        assert sum_result == 3