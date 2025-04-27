import os
import time as T
from typing import Generator,Any,List,Dict
import humanfriendly 
import cloudpickle as CP
import hashlib
import secrets
import json as J
# 
from mictlanx.v4.client import Client as MictlanXClient
from mictlanx.utils.index import Utils as UtilsX
from xolo.utils.utils import Utils as XoloUtils
# 
from axo import Axo,axo_method,axo_task
from axo.contextmanager import ActiveXContextManager
from axo.endpoint import EndpointManagerX
from axo.storage import StorageService
from axo.polymorphisim import FilterXOut, PipeAndFilter,FilterX,SourceX,SinkX,BucketSource,BucketSink,ManagerWorkerX,WorkerX
import pandas as pd


AXO_ENDPOINT_ID            = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL      = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME      = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT   = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT  = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
endpoint_manager = EndpointManagerX(
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


class MixNetXNode(WorkerX):
    def __init__(self, source_bucket_id = None, source_keys: List[str] = None, sink_bucket_id: str= None):
        print("SOURCE_BUCKET_ID", source_bucket_id)
        print("source_keys",source_keys)
        print("SINK",sink_bucket_id)
        self.source = BucketSource().id if source_bucket_id is None else source_bucket_id
        self.source_keys = [] if source_keys is None  else source_keys
        self.sink = BucketSink().id if sink_bucket_id is None else sink_bucket_id
        self.salt = os.urandom(16).hex()
        self.secret_key = "e9a04f2a1f08d7c9c8e60bfa5b1c6b2a5d3c6f4e7093b07c49d6f29a93f8b5e2"
        self.chunk_size = "1mb"
        
    def hash_function_with_salt(self,key:str, salt:str)->int:
        salted_key = key + salt
        return int(hashlib.sha256(salted_key.encode('utf-8')).hexdigest(), 16)

    def create_subclusters(self,*args,**kwargs):
        num_subclusters = int(kwargs.get("num_subclusters",2))
        source_path:str = kwargs.get("source_path")
        print("1. CREATE SUB CLUSTERS", source_path)
        print("1.1 NUM_SUBCLUSTERS", num_subclusters)
        df = pd.read_csv(source_path)
        subclusters = [[] for _ in range(num_subclusters)]
        vvotes = df.to_dict("records")
        for vote in vvotes:
            partition_key = str(vote['vote_id'])
            partition_hash_result = Axo.call(
                instance=self,
                method_name="hash_function_with_salt",
                key = partition_key,
                salt = self.salt
            )
            if partition_hash_result.is_ok:
                partition_hash:str = partition_hash_result.unwrap()
                subcluster_id = partition_hash % num_subclusters
                subclusters[subcluster_id].append(vote)

        for votes in subclusters:
            for i in range(len(votes) - 1, 0, -1):
                j = secrets.randbelow(i + 1)
                votes[i], votes[j] = votes[j], votes[i]
        shuffled_votes    = [vote for subcluster in subclusters for vote in subcluster]
        return shuffled_votes
    def parse_size(size_str: str) -> int:
        units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
        size_str = size_str.upper().strip()
        for unit in units:
            if size_str.endswith(unit):
                return int(float(size_str[:-len(unit)]) * units[unit])
        return int(size_str)  # Fallback if no unit is provided
    def to_chunks(self,*args,**kwargs):
        
        # cs = .parse_size(chunk_size)
        chunk_size = kwargs.get("chunk_size","1mb")
        data       = kwargs.get("data")
        if not data:
            raise Exception("to_chunks: Not data")
        cs = Axo.call(instance=self, method_name="parse_size", chunk_size = chunk_size).unwrap_or(10000000)
        xs = []
        for i in range(0, len(data), cs):
            x = data[i:i + cs]
            xs.append(x)
        return xs
    
    def encryption(self,*args,**kwargs):
        votes = kwargs.get("votes",[])
        votes_bytes = J.dumps(votes).encode("utf-8")
        # print("VOTES_BYTES",votes_bytes)
        secret_key_bytes = bytes.fromhex(self.secret_key)
        print("SECERT_KEY_BYTES", secret_key_bytes)
        encrypted = XoloUtils.encrypt_aes(key=secret_key_bytes,data=votes_bytes)
        if encrypted.is_ok:
            return encrypted.unwrap()
        return encrypted.unwrap_err()
        # encrypted_botes = XoloUtils.encrypt_aes(key=)
    
    def ida(self,*args,**kwargs):
        print("4. IDA")

    @axo_task
    def run(self, *args, **kwargs):
        storage:MictlanXClient = kwargs.get("storage")
        sink_bucket_id = kwargs.get("sink_bucket_id","out")
        source_paths = kwargs.get("source_paths",[])
        num_subclusters = int(kwargs.get("num_subclusters",2))
        rf              = int(kwargs.get("rf",1))
        print("INSTANCEEEEEE",self)
        for source_path in source_paths:
            res = Axo.call(
                instance=self,
                method_name="create_subclusters",
                source_path=source_path,
                num_subclusters= num_subclusters
            )
            if res.is_ok:
                votes  = res.unwrap()
                res = Axo.call(instance=self, method_name="encryption",votes=votes)
                if res.is_ok:
                    encrypted = res.unwrap()
                    chunks_result = Axo.call(instance=self, method_name="to_chunks",data=encrypted, chunk_size = self.chunk_size)
                    if chunks_result.is_ok:
                        chunks:List[bytes] = chunks_result.unwrap()
                        for chunk in chunks:
                            storage_result = storage.put(
                                bucket_id          = sink_bucket_id,
                                value= chunk,
                                replication_factor = rf,
                                tags={
                                    "num_subclusters":str(num_subclusters),
                                    "rf":str(rf)
                                }
                            )
                            print("STORAGE.RESULT",storage_result)
                            if storage_result.is_ok:
                                storage_response = storage_result.unwrap()
                                print("PUT.RESPONSE", storage_response.bucket_id, storage_response.key,storage_response.response_time)

                    
                    # print("CHUNKS",chunks)
       

if __name__ =="__main__":
    mw = ManagerWorkerX(
        source_bucket_id="vaults_source",
        worker_class=MixNetXNode,
        dependencies=["pandas","cryptography==42.0.5","humanfriendly"],
        num_subclusters = 5
        # sink_buckets_ids=[
        #     "mw_sink1",
        #     "mw_sink2",
        #     "mw_sink3",
        # ],

    )
    mw.run()