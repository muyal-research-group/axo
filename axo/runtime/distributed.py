
import string
import time as T
from typing import Optional,Dict,Any
from queue import Queue
from threading import Thread
from weakref import WeakKeyDictionary
# 
from nanoid import generate as nanoid
from option import Result,Ok,Err,Option,NONE
# 
from axo import Axo,axo_method
from axo.runtime.runtime import ActiveXRuntime
from axo.scheduler import Scheduler
from axo.storage.services import MictlanXStorageService,StorageService
from axo.endpoint.manager import DistributedEndpointManager
from axo.endpoint.manager import EndpointManagerP
from axo.log import get_logger
from axo.errors import AxoError,AxoErrorType
from axo.storage import AxoStorage
from axo.storage.loader import AxoLoader
from axo.helpers import serialize_blobs_from_instance

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
        loader_api_globals: Dict[str,Any] = {},
        default_storage_service_params:Dict[str,Any] = {},
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
        self.__storage_service = storage_service if storage_service else MictlanXStorageService(client=default_storage_service_params.get("client",None),uri=default_storage_service_params.get("routers_str","") )
        self.__axo_storage = AxoStorage(storage=self.__storage_service)
        self.__axo_loader = AxoLoader(storage=self.__axo_storage,
            api_globals={**loader_api_globals,"Axo":Axo,"axo_method":axo_method} or {},
            safe_builtins={},
        )
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
    
    @property
    def axo_storage(self)->AxoStorage:
        return self.__axo_storage
    
    @property
    def axo_loader(self)->AxoLoader:
        return self.__axo_loader


    # ------------------------------------------------------------------ #
    # Persist & fetch helpers
    # ------------------------------------------------------------------ #
    async def get_active_object(self, *, bucket_id: str, key: str) -> Result[Axo, AxoError]:
        return await self.__axo_loader.load_object(bucket_id=bucket_id, key=key)

    async def persistify(self, instance: Axo, *, bucket_id: str = "axo", key: Optional[str] = None) -> Result[str, AxoError]:
        try:
            key = key or instance.get_axo_key()

            # 1) endpoint metadata
            endpoint = self.__endpoint_manager.get_endpoint(instance.get_endpoint_id())
            if not endpoint:
                return Err(AxoError.make(AxoErrorType.NOT_FOUND, f"No endpoint found: {instance.get_endpoint_id()}"))
            meta_res = endpoint.put(key=key, value=instance._acx_metadata)
            if meta_res.is_err:
                return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, str(meta_res.unwrap_err())))

            # 2) blobs via AxoStorage
            blobs_res = serialize_blobs_from_instance(instance, bucket_id=bucket_id, key=key)
            if blobs_res.is_err:
                return Err(blobs_res.unwrap_err())
            blobs, class_name = blobs_res.unwrap()

            put_res = await self.__axo_storage.put_blobs(
                bucket_id=bucket_id,
                key=key,
                blobs=blobs,
                class_name=class_name,
            )
            if put_res.is_err:
                return Err(put_res.unwrap_err())

            logger.info({
                "event": "Axo.persistify",
                "mode": "DISTRIBUTED",
                "bucket_id": bucket_id,
                "key": key,
            })
            return Ok(key)
        except AxoError as ax:
            return Err(ax)
        except Exception as e:
            logger.exception("Persistify failed")
            return Err(AxoError.make(AxoErrorType.INTERNAL_ERROR, f"persistify failed: {e}"))


    def stop(self):
        logger.debug("Stop distributed runtime")
    def _handle_put_task(self, task):
        return super()._handle_put_task(task)
