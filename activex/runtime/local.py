from activex.runtime.runtime import ActiveXRuntime
from activex.scheduler import ActiveXScheduler
from activex.endpoint import LocalEndpoint,XoloEndpointManager
from activex.storage.data import LocalStorageService,StorageService
from option import Option, NONE
import logging
from queue import Queue
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

class LocalRuntime(ActiveXRuntime):
    def __init__(self, 
                 runtime_id: str="", 
                 is_distributed: bool = False,
                 maxsize:int=100,
                 storage_service:Option[StorageService] = NONE
    ):
        q= Queue(maxsize= maxsize)
        # default_ss =  (lambda _:  )
        super().__init__(
            q=q,
            endpoint_manager= XoloEndpointManager(endpoints={"activex-local-endpoint-0": LocalEndpoint()}),
            # middleware=LocalEndpoint(),
            storage_service=storage_service.unwrap_or( LocalStorageService(storage_service_id="local-store")),
            scheduler= ActiveXScheduler(
                tasks=[], 
                runtime_queue=q
            ),
            runtime_id=runtime_id,
            is_distributed=is_distributed, 
        )

    def stop(self):
        logger.debug("Stopping the local runtime %s", self.runtime_id)
        