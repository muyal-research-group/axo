
from axo.contextmanager import ActiveXContextManager
import logging
from common import HeatmapProducer
import more_itertools as mit
import sys

logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

args = sys.argv[1:]

def main():
    """ 
        main: Function that run the following steps:

        1. Init the ActiveX handler (important!)
        There are 2 context where the handler can run code: Local and Distributed. 

        Local: Execute the object method locally and save the objects in memory.
        Distributed: Executes the object method on endpoints and save it in an object store.

        The distributed context is in working progress, for now the method execution is local, but
        the object are saved in MictlanX (read more about https://github.com/muyal-research-group/mictlanx-client)

        2. Create an object instance as usual of a Plotter.
        2.1 Perform some number crunching using the add,substract.. methods implemented in Plotter object.
        3. Call the method persistify in the plotter instance to allocate the object in MictlanX
    """
    key:str = mit.nth(args, 0, "")

    logger.debug("Step 1. init ActiveX object handler")
    _ = ActiveXContextManager.distributed()
    logger.debug("Step 2. create an object instance")
    obj:HeatmapProducer = HeatmapProducer.get_by_key(key=key).unwrap()
    print(obj.df)
    
if __name__  == "__main__":
    main()
