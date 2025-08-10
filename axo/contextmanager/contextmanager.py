from axo.runtime import set_runtime,get_runtime
from axo.runtime.local import LocalRuntime
from axo.runtime.distributed import DistributedRuntime
from axo.runtime.runtime import ActiveXRuntime
# 
from axo.endpoint.manager import DistributedEndpointManager
# 
from axo.storage.data import StorageService,LocalStorageService
# 
from axo.scheduler import AxoScheduler
#
from axo.log import get_logger

from option import Option,NONE
from typing import Optional,Union
from nanoid import generate as nanoid
from queue import Queue
import string
import time as T
logger = get_logger(name= __name__)
class AxoContextManager:
    is_running = False
    def __init__(self, runtime:Optional[ActiveXRuntime] = None):
        self.start_time = T.time()
        self.prev_runtime = None
        self.runtime = runtime
        # self.prev_runtime = get_runtime()
        # if runtime == None:
        #     runtime_id = "local-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits))
        #     self.runtime = LocalRuntime(
        #         storage_service= LocalStorageService(storage_service_id=f"{runtime_id}_storage"),
        #         runtime_id=runtime_id
        #     )
        # else:
        #     self.runtime = runtime
        # set_runtime(self.runtime)
        # self.is_running = True

    @staticmethod
    def local()->'AxoContextManager':
        suffix = nanoid(alphabet=string.ascii_lowercase+string.digits)
        return AxoContextManager(
            runtime= LocalRuntime(
                runtime_id      = "local-{}".format(suffix),
                storage_service = LocalStorageService(storage_service_id=f"local-storage-{suffix}")
            )
        )
        
    @staticmethod
    def distributed(
        endpoint_manager:DistributedEndpointManager,
        storage_service:Option[StorageService] = NONE,
        maxsize:int =100
    )->'AxoContextManager':
        runtime_id = "distributed-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits))
        q = Queue(maxsize=maxsize)
        return AxoContextManager(
            runtime= DistributedRuntime(
                runtime_id=runtime_id,
                q = q,
                endpoint_manager=endpoint_manager,
                storage_service=storage_service, 
                scheduler = AxoScheduler(
                    runtime_queue=q,
                )
            )
        )
        
    def stop(self):
        # print("SOTP", self.is_running,self.prev_runtime)
        get_mode = lambda x: "DISTRIBUTED" if x  else "LOCAL"
        logger.debug({
            "event":"CONTEXT.MANAGER.STOP",
            "mode":get_mode(self.runtime.is_distributed) if  self.runtime else "NONE",
            "is_running":int(self.is_running),
            "service_time": T.time() - self.start_time
        })
        if not self.is_running:
            return
        self.runtime.stop()
        set_runtime(self.prev_runtime)
        self.is_running=False

    # ---------------------------------------------------------------- #
    # Context‐manager protocol
    # ---------------------------------------------------------------- #
    def __enter__(self)->Union[LocalRuntime,DistributedRuntime]:
        self.prev_runtime = get_runtime()
        # runtime is built by the factories below; if None, build a default local one
        if self.runtime is None:
            suffix = nanoid(alphabet=string.ascii_lowercase + string.digits)
            self.runtime = LocalRuntime(
                runtime_id=f"local-{suffix}",
                storage_service=LocalStorageService(storage_service_id=f"local-storage-{suffix}")
            )
        set_runtime(self.runtime)
        return self.runtime   # or `return self` if you prefer `as manager`

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        # returning False will re-raise any exception; True would swallow it
        return False

    # def __del__(self):
        # self.stop()