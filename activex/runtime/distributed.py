from activex.runtime.runtime import ActiveXRuntime
from activex.scheduler import ActiveXScheduler
from activex.storage.data import MictlanXStorageService
from activex.endpoint import DistributedEndpoint,XoloEndpointManager
from option import Option,NONE
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams
import logging
from queue import Queue

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)
class DistributedRuntime(ActiveXRuntime):
    def __init__(self, 
                 runtime_id: str,
                 endpoint_manager:XoloEndpointManager=XoloEndpointManager(), 
                 tezcanalyticx_params:Option[TezcanalyticXParams]= NONE,
                 maxsize:int = 100
    ):
        q = Queue(maxsize=maxsize)
        super().__init__(
            q= q,
            runtime_id=runtime_id,
            endpoint_manager=endpoint_manager,
            storage_service=MictlanXStorageService(tezcanalyticx_params=tezcanalyticx_params),
            is_distributed=True,
            scheduler= ActiveXScheduler(tasks=[],runtime_queue=q)
        )
    
    def stop(self):
        logger.debug("Stop distributed runtime")