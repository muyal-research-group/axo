import pytest 
import time as T
from axo.contextmanager import AxoContextManager
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.services import MictlanXStorageService
from option import Some
from .objects.scenario1 import HillClimb
# from .test.objects.scenario1 import *


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
@pytest.mark.asyncio
async def test_local():
    with AxoContextManager.local(): 
        hc:HillClimb   = HillClimb()
        (x_opt,fx_opt) = hc.hill_climb(100).unwrap()
        # _              = hc.plot(x_opt=x_opt,fx_opt=fx_opt)
        

        

    @pytest.mark.skip("")
    @pytest.mark.asyncio
    async def test_distributed(dem: DistributedEndpointManager):
        
        acm = AxoContextManager.distributed(
            endpoint_manager = dem,
            storage_service  = Some(
                MictlanXStorageService(protocol="http")
            )
        )
