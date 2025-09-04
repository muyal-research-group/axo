from axo.contextmanager import AxoContextManager
import logging
import os
import more_itertools as mit
import sys
from axo.contextmanager.contextmanager import AxoContextManager
from axo.endpoint import EndpointManagerX
# from common import Calculator
# from activex.runtime.local import LocalRuntime

AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))

logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

args = sys.argv[1:]

endpoint_manager = EndpointManagerX()
endpoint_manager.add_endpoint(
    endpoint_id=AXO_ENDPOINT_ID,
    protocol=AXO_ENDPOINT_PROTOCOL,
    hostname=AXO_ENDPOINT_HOSTNAME,
    req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
    pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
)
axocm              = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
    
def main():
    """ 
        main: Function that run the following steps:

        1. Init the ActiveX handler (important!)
        There are 2 context where the handler can run code: Local and Distributed. 

        Local: Execute the object method locally and save the objects in memory.
        Distributed: Executes the object method on endpoints and save it in an object store.

        The distributed context is in working progress, for now the method execution is local, but
        the object are saved in MictlanX (read more about https://github.com/muyal-research-group/mictlanx-client)

        2. Create an object instance as usual of a calculator.
        2.1 Perform some number crunching using the add,substract.. methods implemented in Calculator object.
        3. Call the method persistify in the calculator instance to allocate the object in MictlanX
    """
    key:str = mit.nth(args, 0, "calculatorexample02")
    n_objects = int(mit.nth(args, 1,1))
    for i in range(n_objects):
        x,y = 1000,80
        obj = Calculator(x=x*i,y=y*i)
        res = obj.persistify(bucket_id="activex",key=key)
        print("RESULT",res)
    # result = obj.add(x,y)
    # print("METHOD_RESULT", result)
if __name__  == "__main__":
    main()
