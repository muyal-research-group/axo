from activex.polymorphisim import FilterX, PipeAndFilter, BucketSink,BucketSource,FilterXOut
import time as T
from activex import ActiveX,activex_method
from typing import Generator,Any,List,Dict
from activex.contextmanager import ActiveXContextManager
import humanfriendly as  HF
from activex.endpoint import XoloEndpointManager
from activex.storage import StorageService
import cloudpickle as CP
from mictlanx.v4.client import Client as MictlanXClient
from mictlanx.utils.index import Utils as UtilsX
from activex.polymorphisim import FilterXOut, PipeAndFilter,FilterX
import os 


AXO_ENDPOINT_ID            = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL      = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME      = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT   = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT  = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
MICTLANX_CLIENT_ID         = os.environ.get("MICTLANX_CLIENT_ID","client-0-activex")
MICTLANX_DEFAULT_BUCKET_ID = os.environ.get("MICTLANX_DEFAULT_BUCKET_ID","jbddhwnqf606qgwr1ix42vayqmsl1ejl")
MICTLANX_DEBUG             = bool(int(os.environ.get("MICTLANX_DEBUG","1")))
MICTLANX_MAX_WORKERS       = int(os.environ.get("MICTLANX_MAX_WORKERS","2"))
MICTLANX_LOG_PATH          = os.environ.get("MICTLANX_LOG_PATH","./log")
SOURCE_PATH                = os.environ.get("SOURCE_PATH","./source")
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


class IdentityFilter(FilterX):
    @activex_method
    def run(self, *args, **kwargs) -> FilterXOut:
        try:
            source_bucket_id       = kwargs.get("source_bucket_id")
            source_key             = kwargs.get("source_key")
            sink_bucket_id         = kwargs.get("sink_bucket_id")
            chunk_size             = kwargs.get("chunk_size","1MB")
            storage:StorageService = kwargs.get("storage")
            get_res                = storage.get_streaming(bucket_id=source_bucket_id, key=source_key,chunk_size=chunk_size)
            if get_res.is_ok:
                (bytes_iter, metadata) = get_res.unwrap()
                put_res = storage.put_streaming(
                    bukcet_id=sink_bucket_id,
                    key=source_key,
                    data= bytes_iter,
                    tags=metadata.get("tags",{})
                )
            return FilterXOut(
                source_bukcet_id=source_bucket_id,
                sink_bucket_id=sink_bucket_id,
                response_time = 0
            )
        except Exception as e:
            print("EXCEPTION",e)
            return e

if __name__ == "__main__":
    # Define the pattern
    paf = PipeAndFilter()
    # Add source
    paf.add_source(source = BucketSource("xxx"))
    # Add sink
    paf.add_sink(sink = BucketSink("yyy"))
    # Add a filter
    paf.add_filter(IdentityFilter())
    paf.add_filter(IdentityFilter())
    print(str(paf))
    result = paf.run()
    print(result)