from axo.runtime.core import ActiveXRuntime
from axo.scheduler import AxoScheduler
from axo.storage.data import MictlanXStorageService

from axo.endpoint.manager import DistributedEndpointManager
from option import Option,NONE
import logging
from queue import Queue

logger = logging.getLogger("activex.runtime.distributed1")
logger.propagate = False
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

class DistributedRuntime(ActiveXRuntime):
    def __init__(self, 
                 runtime_id: str,
                 endpoint_manager: DistributedEndpointManager,
                 maxsize:int = 100
    ):
        q = Queue(maxsize=maxsize)
        super().__init__(
            q= q,
            runtime_id=runtime_id,
            endpoint_manager=endpoint_manager,
            storage_service=MictlanXStorageService(),
            is_distributed=True,
            scheduler= AxoScheduler(tasks=[],runtime_queue=q)
        )
    
    def stop(self):
        logger.debug("Stop distributed runtime")