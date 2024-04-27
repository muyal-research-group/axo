from activex.runtime.runtime import ActiveXRuntime
from activex.scheduler import ActiveXScheduler
from activex.storage.data import MictlanXStorageService
from activex.storage.metadata import ActiveXMetadataService
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
                 protocol:str = "tcp",
                 hostname:str = "127.0.0.1",
                 port:int = 60667,
                 maxsize:int = 100
    ):
        q = Queue(maxsize=maxsize)
        super().__init__(
            q= q,
            runtime_id=runtime_id,
            metadata_service=ActiveXMetadataService(
                protocol = protocol,
                hostname = hostname,
                port     = port
            ),
            storage_service=MictlanXStorageService(),
            is_distributed=True,
            scheduler= ActiveXScheduler(tasks=[],runtime_queue=q)
        )
    
    def stop(self):
        logger.debug("Stop distributed runtime")