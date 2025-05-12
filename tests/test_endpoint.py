import pytest 
from axo.endpoint.endpoint import LocalEndpoint,DistributedEndpoint
import asyncio


from axo import Axo
class Calc(Axo):
    def sum(x:int,y:int):
        return x+y




@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_local_endpoint():
    le = LocalEndpoint(endpoint_id="e1")
    calc = Calc()
    res = le.method_execution(key="sum",fname="sum",ao = calc, f = getattr(calc,"sum"),fargs=[1,1],fkwargs={})
    print(res)


# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_distributed_endpoint():
    le = DistributedEndpoint(endpoint_id="activex-endpoint-0")
    calc = Calc(
        sink_bucket_id = "TEST"
    )
    # print("RESULT",res)
    # await asyncio.sleep(1000)
