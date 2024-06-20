
import time as T
from activex import ActiveX,activex_method
from typing import Generator,Any,List
from activex.contextmanager import ActiveXContextManager
import humanfriendly as  HF
from activex.endpoint import XoloEndpointManager
import cloudpickle as CP
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils as UtilsX
from concurrent.futures import ProcessPoolExecutor,as_completed

import logging
import os 
import inspect

import numpy as np
from numba import jit
import galois

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
client = Client(
    client_id      = MICTLANX_CLIENT_ID,
    routers         = routers,
    debug           = MICTLANX_DEBUG,
    max_workers     = MICTLANX_MAX_WORKERS,
    bucket_id       = MICTLANX_DEFAULT_BUCKET_ID,
    log_output_path = MICTLANX_LOG_PATH    
)



class Splitter(ActiveX):
    def __init__(self):
        pass
    @activex_method
    def to_chunks(self,chunk_size:int,*args,**kwargs):
        print("PUT_BUTES", self.put_bytes)
        print("GETBYTES", self.get_bytes)
        axo_sink_path_sink_bucket_id_path = kwargs.get("axo_sink_path_sink_bucket_id_path","/sink/")
        axo_sink_key:str = kwargs.get("axo_sink_key","sharex")
        axo_source_path  = kwargs.get("axo_source_path","/source/{}".format(axo_sink_key))
        # print(self.pu)
        # cs = HF.parse_size(chunk_size)
        with open(axo_source_path, "rb") as f:
            index = 0 
            while True:
                data = f.read(chunk_size)
                if data:
                    path = "{}/{}_{}".format(axo_sink_path_sink_bucket_id_path, axo_sink_key,index)
                    print("WRITE", path)
                    print("SIZE",len(data))
                    print("_"*10)
                    index+=1
                    with open(path,"wb") as f2:
                        f2.write(data)



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
    s = Splitter()
    x = s.persistify()
    res=  s.to_chunks(
        chunk_size=HF.parse_size("10kb"),
        source_bucket_id = "xxx"
    )
    print("RES",res)
    
    # print(s)
    # s.to_chunks(chunk_size="10kb")

if __name__ =="__main__":
    main()