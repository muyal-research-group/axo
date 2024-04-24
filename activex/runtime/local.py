from activex.runtime.runtime import ActiveXRuntime
from activex.storage.metadata import LocalMetadataService
from activex.storage.data import LocalStorageService
import logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

class LocalRuntime(ActiveXRuntime):
    def __init__(self, runtime_id: str="", is_distributed: bool = False):
        super().__init__(
            metadata_service=LocalMetadataService(),
            storage_service=LocalStorageService(storage_service_id="local-store"),
            runtime_id=runtime_id,
            is_distributed=is_distributed
        )

    def stop(self):
        logger.debug("Stopping the local runtime %s", self.runtime_id)
        