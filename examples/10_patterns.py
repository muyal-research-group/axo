import time as T
from activex import Axo,axo_method
from typing import Generator,Any,List,Dict
from activex.contextmanager import ActiveXContextManager
import humanfriendly as  HF
from activex.endpoint import XoloEndpointManager
from activex.storage import StorageService
import cloudpickle as CP
from mictlanx.v4.client import Client as MictlanXClient
from mictlanx.utils.index import Utils as UtilsX
from activex.polymorphisim import FilterXOut, PipeAndFilter,FilterX,SourceX,SinkX,BucketSource,BucketSink
import os


AXO_ENDPOINT_ID            = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL      = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME      = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT   = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT  = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))

# MICTLANX_CLIENT_ID         = os.environ.get("MICTLANX_CLIENT_ID","client-0-activex")
# MICTLANX_DEFAULT_BUCKET_ID = os.environ.get("MICTLANX_DEFAULT_BUCKET_ID","jbddhwnqf606qgwr1ix42vayqmsl1ejl")
# MICTLANX_DEBUG             = bool(int(os.environ.get("MICTLANX_DEBUG","1")))
# MICTLANX_MAX_WORKERS       = int(os.environ.get("MICTLANX_MAX_WORKERS","2"))
# MICTLANX_LOG_PATH          = os.environ.get("MICTLANX_LOG_PATH","./log")
# SOURCE_PATH                = os.environ.get("SOURCE_PATH","./source")


class SplitterFilter(FilterX):
    def __init__(self,source:SourceX=BucketSource(), sink:SinkX=BucketSink()):
        FilterX.__init__(self,source=source,sink=sink)
    @axo_method
    def run(self, *args, **kwargs) -> FilterXOut:
        sink_bucket_id   = kwargs.get("sink_bucket_id")
        source_bucket_id = kwargs.get("source_bucket_id")
        storage:MictlanXClient          = kwargs.get("storage")
        print("*"*50)
        print("STORAGE", storage)
        print("*"*50)
        start_time = T.time()

        return FilterXOut(
            sink_bucket_id=sink_bucket_id,
            source_bukcet_id= source_bucket_id,
            response_time=T.time() - start_time
        )


def main():
    endpoint_manager = XoloEndpointManager(
        endpoint_id=AXO_ENDPOINT_ID,
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
    sf = SplitterFilter(
        source=BucketSource(bucket_id="dlwfwz8orsnr6r0plv8up0z6yquu4nj2")
    )
    res = sf.run()
    print("RES",res)
    # pandf = PipeAndFilter()
    # pandf.add_filter(sf)
    # result  = pandf.run()
    # print("RESULT", result)
    # print("SF", sf)
    # print(sf.run())


if __name__ =="__main__":
    main()