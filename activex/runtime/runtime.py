from abc import ABC,abstractmethod
from typing import Optional,Dict,List
from nanoid import generate as nanoid
import string
from weakref import WeakKeyDictionary
# from activex.storage.metadata import MetadataService
from activex.storage.data import StorageService,ActiveX
from activex.endpoint import EndpointX,XoloEndpointManager
from activex.scheduler import Scheduler,Task
from option import Result,Err,Ok
from queue import Queue
from threading import Thread
import logging
import time as T

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

ALPHABET = string.digits+string.ascii_lowercase

class ActiveXRuntime(ABC,Thread):
    def __init__(self,
                 q:Queue,
                 endpoint_manager:XoloEndpointManager,
                 storage_service:StorageService,
                 scheduler:Scheduler,
                 runtime_id:str,
                 is_distributed:bool = False ,
    ):
        Thread.__init__(self,daemon=True, name="activex-runtime")
        self.runtime_id          = runtime_id if len(runtime_id) > 16 else nanoid(alphabet=ALPHABET)
        self.is_distributed:bool = is_distributed
        self.inmemory_objects    = WeakKeyDictionary()
        self.remote_files        = set()
        # self.middleware          = middleware
        self.storage_service     = storage_service
        self.q                   = q
        self.scheduler:Scheduler = scheduler
        self.is_running = True
        self.endpoint_manager= endpoint_manager
        self.start()
    
    def get_by_key(self,bucket_id:str,key:str)->Result[ActiveX,Exception]:
        return self.storage_service.get(key=key,bucket_id=bucket_id)
    def persistify(
            self,
            instance: ActiveX,
            bucket_id:Optional[str] = None,
            key:Optional[str] = None,
            storage_node:Optional[str] = None
    )->Result[str,Exception]:
        
        instance_endpoint_id = instance.get_endpoint_id()
        endpoint = self.endpoint_manager.get_endpoint(endpoint_id=instance_endpoint_id )
        logger.debug({
            "event":"GET.ENDPOINT",
            "instance_endpoint_id":instance_endpoint_id,
            "endpoint_id":endpoint.endpoint_id,
            "hostname":endpoint.hostname,
            "pubsub_port":endpoint.pubsub_port,
            "req_res_port":endpoint.req_res_port
        })
        # print("ENDPOINT",endpoint)
        m_result = endpoint.put(
            key=key,
            metadata=instance._acx_metadata
        )
        s_result = self.storage_service.put(
            obj=instance,
            bucket_id=bucket_id,
            key=key
        )
        return Ok(key)
            
        # logger.debug("%s persistify",key)

    @abstractmethod
    def stop(self):
        pass
    def run(self) -> None:
        while self.is_running:
            task:Task = self.q.get()
            current_time = T.time()
            logger.debug("TASK.DEQUEUE {}".format(task.id))
            if current_time >= task.executes_at:
                path   = task.metadata.get("path","")
                if task.operation == "PUT" and not path =="" and not path in self.remote_files:
                    result = self.storage_service.put_data_from_file(key="",source_path=path,tags={},chunk_size="1MB")
                    if result.is_ok:
                        self.remote_files.add(path)
                    logger.debug("{} {}".format(task.operation, task.id ))
                elif task.operation == "DROP":
                    logger.debug("{} {}".format(task.operation, task.id))

        