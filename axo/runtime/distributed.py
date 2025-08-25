from axo.runtime.runtime import ActiveXRuntime
from axo.scheduler import Scheduler
from axo.storage.services import MictlanXStorageService,StorageService
from axo.endpoint.manager import DistributedEndpointManager
from option import Option,NONE
from axo.endpoint.manager import EndpointManagerP
from axo.endpoint.endpoint import EndpointX
from queue import Queue
from axo.log import get_logger
from nanoid import generate as nanoid
from threading import Thread
from weakref import WeakKeyDictionary
import string
from axo import Axo
from option import Result,Ok,Err
from typing import Optional
import time as T
import cloudpickle as CP
logger = get_logger(name= __name__)
_ALPHABET = string.digits + string.ascii_lowercase

    
class DistributedRuntime(ActiveXRuntime,Thread):
    def __init__(
        self,
        *,
        q: Queue,
        # endpoint_manager: EndpointManagerP[EndpointX],
        endpoint_manager: DistributedEndpointManager,
        storage_service:Option[StorageService] = NONE,
        scheduler: Scheduler,
        # is_distributed: bool = False,
        runtime_id: str="",
    ) -> None:
        super().__init__(daemon=True)

        # identifiers & flags -------------------------------------------
        self.__runtime_id = (
            runtime_id if len(runtime_id) >= 16 else nanoid(alphabet=_ALPHABET)
        )

        # collaborators --------------------------------------------------
        self.__q = q
        self.__scheduler = scheduler
        self.__storage_service = storage_service.unwrap_or(MictlanXStorageService(protocol="http",log_path="/log"))
        self.__endpoint_manager = endpoint_manager

        # caches ---------------------------------------------------------
        self.__inmemory_objects: WeakKeyDictionary[Axo, None] = WeakKeyDictionary()
        self.remote_files: set[str] = set()


        # thread ctl -----------------------------------------------------
        self.__is_running = True
        self.__is_distributed = True
        self.start()

    @property
    def runtime_id(self):
        self.__runtime_id
    @property
    def q(self):
        return self.__q
    @property
    def is_distributed(self)->bool:
        return self.__is_distributed
    @property
    def scheduler(self)->Scheduler:
        return self.__scheduler
    @property
    def storage_service(self)->StorageService:
        return self.__storage_service
    @property
    def endpoint_manager(self)->EndpointManagerP:
        return self.__endpoint_manager
    @property
    def inmemory_objects(self)->WeakKeyDictionary[Axo,None]:
        return self.__inmemory_objects
    @property
    def is_running(self)->bool:
        return self.__is_running  


    # ------------------------------------------------------------------ #
    # Persist & fetch helpers
    # ------------------------------------------------------------------ #
    async def get_active_object(self, *, bucket_id: str, key: str) -> Result[Axo, Exception]:
        return await self.storage_service._get_active_object(bucket_id=bucket_id, key=key)

    async def persistify(
        self, instance: Axo, *, bucket_id: str = "axo", key: Optional[str] = None
    ) -> Result[str, Exception]:
        """
        Serialize an active object and store (1) its pickled class‐def, and
        (2) its state bytes.  Uses whatever endpoint manager was injected.
        """
        try:
            t1  = T.time()
            key = key or instance.get_axo_key()
            endpoint:EndpointX = self.endpoint_manager.get_endpoint(instance.get_endpoint_id())
            meta_res:Result[str, Exception] = endpoint.put(key=key, value=instance._acx_metadata)
            if meta_res.is_err:
                logger.error({
                    "error":str(meta_res.unwrap_err())
                })
                return Err(meta_res.unwrap_err())
            logger.info({
                "event":"ENDPOINT.PUT",
                "response_time":T.time() - t1
            })
            raw_parts_result = instance.get_raw_parts()
            if raw_parts_result.is_err:
                return Err(raw_parts_result.unwrap_err())
            
            attrs, class_code = raw_parts_result.unwrap()
            # print("HREE",attrs)
            attrs_put_result = await self.storage_service.put(
                bucket_id=bucket_id,
                key = f"{key}_attrs",
                tags = {
                    "module": instance._acx_metadata.axo_module,
                    "class_name": instance._acx_metadata.axo_class_name,
                }, 
                data =CP.dumps(attrs)
            )
            if attrs_put_result.is_err:
                return Err(attrs_put_result.unwrap_err())

            tags = instance._acx_metadata.to_tags()
            class_code_put_result = await self.storage_service.put(
                bucket_id=bucket_id,
                key = f"{key}_source_code",
                tags = {
                    **tags
                }, 
                data = CP.dumps(class_code.encode("utf-8"))
            )
            if class_code_put_result.is_err:
                return Err(class_code_put_result.unwrap_err())
            return Ok(key)
        except Exception as e:
            logger.exception(f"Persistify failed: {e}")
            return Err(e)
        # pass

    def stop(self):
        logger.debug("Stop distributed runtime")
    def _handle_put_task(self, task):
        return super()._handle_put_task(task)
