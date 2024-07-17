from .common import Dog
import os
from option import Result,Ok,Err
import unittest as UT
from activex import ActiveX,activex_method
from activex.contextmanager.contextmanager import ActiveXContextManager
from activex.endpoint import XoloEndpointManager,EndpointX
# from activex.runtime.local import LocalRuntime

AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))


    
class AxoBasics(UT.TestCase):

    # def test_get_object(self):
    #     x =ActiveX.get_object_parts(
    #         raw_obj=b""
    #     )

    @UT.skip("")
    def test_instance(self):
        endpoint_manager = XoloEndpointManager()
        endpoint_manager.add_endpoint(
            endpoint_id=AXO_ENDPOINT_ID,
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )
        ax              = ActiveXContextManager.distributed(endpoint_manager=endpoint_manager)
        rex = Dog(name="Rex")
        rex.bark(name="Rory")
        print(rex)
        

if __name__ == "__maain__":
    UT.main()