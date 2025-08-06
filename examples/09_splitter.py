
import time as T
from axo import Axo,axo_method
from typing import Generator,Any,List,Dict
from axo.contextmanager import AxoContextManager
import humanfriendly as  HF
from axo.endpoint import EndpointManagerX
from axo.storage import StorageService
import cloudpickle as CP
from mictlanx.v4.client import Client as MictlanXClient
from mictlanx.utils.index import Utils as UtilsX
from concurrent.futures import ProcessPoolExecutor,as_completed
import os 

import numpy as np
from numba import jit
AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
MICTLANX_CLIENT_ID         = os.environ.get("MICTLANX_CLIENT_ID","client-0-activex")
MICTLANX_DEFAULT_BUCKET_ID = os.environ.get("MICTLANX_DEFAULT_BUCKET_ID","jbddhwnqf606qgwr1ix42vayqmsl1ejl")
MICTLANX_DEBUG             = bool(int(os.environ.get("MICTLANX_DEBUG","1")))
MICTLANX_MAX_WORKERS       = int(os.environ.get("MICTLANX_MAX_WORKERS","2"))
MICTLANX_LOG_PATH          = os.environ.get("MICTLANX_LOG_PATH","./log")
SOURCE_PATH                = os.environ.get("SOURCE_PATH","./source")
routers = list(UtilsX.routers_from_str(os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
client = MictlanXClient(
    client_id      = MICTLANX_CLIENT_ID,
    routers         = routers,
    debug           = MICTLANX_DEBUG,
    max_workers     = MICTLANX_MAX_WORKERS,
    bucket_id       = MICTLANX_DEFAULT_BUCKET_ID,
    log_output_path = MICTLANX_LOG_PATH    
)


# class Context:
#     def __init__(self):
#         parameters:Dict[str,Any] = {}
#     @staticmethod
#     def default()->'Context':
#         return Context()


class Splitter(Axo):
    def __init__(self):
        pass
    @axo_method
    def to_chunks(self,*args,**kwargs):
        storage:StorageService            = kwargs.get("mictlanx")
        chunk_size                        = kwargs.get("chunk_size","1MB")
        axo_sink_path_sink_bucket_id_path = kwargs.get("axo_sink_path_sink_bucket_id_path","/sink/")
        source_bucket_id                  = kwargs.get("source_bucket_id","source_bucket_id")
        sink_bucket_id                    = kwargs.get("sink_bucket_id","sink_bucket_id")
        source_key                        = kwargs.get("source_key","source_key")
        sink_key                          = kwargs.get("sink_key","sink_key")
        res                               = storage.get_streaming(bucket_id=source_bucket_id, key=source_key,chunk_size=chunk_size)
        if res.is_ok:
            (data_stream,metadata) = res.unwrap()
            i =0 
            for chunk in data_stream:
                chunk_id = "{}_{}".format(source_key,i)
                res = storage.put(bucket_id=sink_bucket_id, key=chunk_id,data=chunk)
                path = "{}/{}".format(axo_sink_path_sink_bucket_id_path, chunk_id)
                if res.is_ok:
                    print(chunk_id,"was written successfully", res)
                    i+=1
        else:
            raise res.unwrap_err()
        return b"ToCHUNKS"




def main():
    endpoint_manager = EndpointManagerX(
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
    axcm = AxoContextManager.distributed(
        endpoint_manager= endpoint_manager
    )
    # axcm = ActiveXContextManager.local()
    s = Splitter()
    x = s.persistify()
    print("x",x)
    res=  s.to_chunks(
        chunk_size="10kb",
        source_bucket_id = "xxx",
        sink_bucket_id  = "my_output",
        # max_workers = 2
    )
    # xxx -> to_chunks -> my_output

if __name__ =="__main__":
    main()