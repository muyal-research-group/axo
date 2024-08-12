import os
import time as T
from typing import Generator,Any,List,Dict
import humanfriendly as  HF
import cloudpickle as CP
# 
from mictlanx.v4.client import Client as MictlanXClient
from mictlanx.utils.index import Utils as UtilsX
# 
from activex import Axo,axo_method,axo_task
from activex.contextmanager import ActiveXContextManager
from activex.endpoint import XoloEndpointManager
from activex.storage import StorageService
from activex.polymorphisim import FilterXOut, PipeAndFilter,FilterX,SourceX,SinkX,BucketSource,BucketSink,ManagerWorkerX,WorkerX


AXO_ENDPOINT_ID            = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL      = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME      = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT   = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT  = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
endpoint_manager = XoloEndpointManager(
    endpoint_id="mw_example",
    endpoints={}
)
endpoint_manager.add_endpoint(
    endpoint_id= AXO_ENDPOINT_ID,
    hostname=AXO_ENDPOINT_HOSTNAME,
    protocol=AXO_ENDPOINT_PROTOCOL,
    pubsub_port=AXO_ENDPOINT_PUBSUB_PORT,
    req_res_port=AXO_ENDPOINT_REQ_RES_PORT
)
axcm = ActiveXContextManager.distributed(
    endpoint_manager= endpoint_manager
)


class RushP(WorkerX):
    def __init__(self, source_bucket_id = None, source_keys: List[str] = None, sink_bucket_id: str= None):
        self.source = BucketSource().id if source_bucket_id is None else source_bucket_id
        self.source_keys = [] if source_keys is None  else source_keys
        self.sink = BucketSink().id if sink_bucket_id is None else sink_bucket_id
        # print()
        # super().__init__(source, source_keys, sink)
    @axo_task
    def run(self, *args, **kwargs):
        print("=="*50,"WORKER TASK!!")

if __name__ =="__main__":
    mw = ManagerWorkerX(
        source_bucket_id="vaults_source",
        workers=[
            RushP(),
            RushP(),
            RushP()
        ],
        sink_buckets_ids=[
            "mw_sink1",
            "mw_sink2",
            "mw_sink3",
        ]
    )
    mw.run()