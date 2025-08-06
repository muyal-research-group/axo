import pytest
from axo import Axo,axo_method
from axo.contextmanager import AxoContextManager
from axo.endpoint.manager import DistributedEndpointManager

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
@pytest.mark.skip("")
def test_local():
    # dem = AxoContextManager.local()
    with AxoContextManager.local() as cmx:
        c:Calculator = Calculator()

        res = c.sum([0,1,2])


        print(res)


@pytest.mark.asyncio
async def test_distributed(dem:DistributedEndpointManager):
    with AxoContextManager.distributed( endpoint_manager= dem) as cmx:
        c:Calculator = Calculator(axo_endpoint_id = "axo-endpoint-0")
        res = await c.persistify()
        print("RESULT",res)
        assert res.is_ok
        res = c.sum([0,1,2])
        print(res)
        res = c.substract([0,1,2])
        print(res)
        res = c.mult([3,2,1])
        print(res)
        res = c.divide([3,2,1])
        print(res)

