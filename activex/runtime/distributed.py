from activex.runtime.runtime import ActiveXRuntime
from activex.storage.data import MictlanXStorageService
from activex.storage.metadata import ActiveXMetadataService
import logging
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
                 port:int = 60667
    ):
        super().__init__(
            runtime_id=runtime_id,
            metadata_service=ActiveXMetadataService(
                protocol = protocol,
                hostname = hostname,
                port     = port
            ),
            storage_service=MictlanXStorageService(),
            is_distributed=True
        )
    
    def stop(self):
        logger.debug("Stop distributed runtime")