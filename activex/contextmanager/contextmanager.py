from activex.runtime import set_runtime,get_runtime
from activex.runtime.local import LocalRuntime
from activex.runtime.distributed import DistributedRuntime
from activex.runtime.runtime import ActiveXRuntime
from activex.endpoint import XoloEndpointManager
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams
from option import Option,NONE
from typing import Optional
from nanoid import generate as nanoid
import string
class ActiveXContextManager:
    is_running = False
    def __init__(self, runtime:Optional[ActiveXRuntime] = None):
        self.prev_runtime = get_runtime()
        if runtime == None:
            self.runtime = LocalRuntime(
                runtime_id="local-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits))
            )
        else:
            self.runtime = runtime
        set_runtime(self.runtime)
        self.is_running = True

    @staticmethod
    def local()->'ActiveXContextManager':
        return ActiveXContextManager(
            runtime= LocalRuntime(
                runtime_id="local-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits)),
            )
        )
        
    @staticmethod
    def distributed(
        endpoint_manager:XoloEndpointManager,
        tezcanalyticx_params:Option[TezcanalyticXParams] = NONE
    )->'ActiveXContextManager':
        return ActiveXContextManager(
            runtime= DistributedRuntime(
                runtime_id="distributed-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits)),
                endpoint_manager=endpoint_manager,
                tezcanalyticx_params=tezcanalyticx_params
            )
        )
        
    def stop(self):
        if not self.is_running:
            return
        self.runtime.stop()
        set_runtime(self.prev_runtime)
        self.is_running=False

    def __del__(self):
        self.stop()