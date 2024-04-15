from typing import Dict
import unittest
import cloudpickle as CP
from activex.v1 import ActiveX
from activex.decorators import activex
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse
from option import Result
import os
NODE_ID = os.environ.get("NODE_ID","activex")
BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",NODE_ID)
routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")
OBSERVATORY_INDEX  = int(os.environ.get("OBSERVATORY_INDEX",0))
MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","http")
OUTPUT_PATH:str    = os.environ.get("OUTPUT_PATH","/test/risk_calculator/sink")
# Parse the peers_str to a Peer object

routers     = list(Utils.routers_from_str(routers_str,protocol=MICTLANX_PROTOCOL))
c = Client(
    # Unique identifier of the client
    client_id   = os.environ.get("MICTLANX_CLIENT_ID","client-0"),
    # Storage peers
    routers     = routers,
    # Number of threads to perform I/O operations
    max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
    # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
    debug       = True,
    log_output_path= os.environ.get("MICTLANX_LOG_OUTPUT_PATH","./log"),
    bucket_id=BUCKET_ID
)




class Dog(ActiveX):
    def __init__(self, object_id: str, tags: Dict[str, str] = ...):
        super().__init__(object_id, tags)
        self.dog_name = "PERRITO"
    @activex
    def bark(self):
        print("WOOF")
        

class ActiveXTest(unittest.TestCase):

    def test_serialize():
        dog_obj = Dog()
        dog_obj_bytes = dog_obj.to_bytes()
        future = c.put(
            key= dog_obj.object_id, # if empty it uses the sha256(obj_bytes)
            value=dog_obj_bytes,
            tags={
                "more":"metadata",
                "about":"the object"
            }
        )
        response:Result[PutResponse,Exception]= future.result()
        if response.is_ok:
            print("ActiveX object is saves successfully...")

        # pf = PerroFeo() 
