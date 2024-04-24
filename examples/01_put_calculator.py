from activex.handler import ActiveXHandler
from common import Calculator
import logging
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

        2. Create an object instance as usual of a calculator.
        2.1 Perform some number crunching using the add,substract.. methods implemented in Calculator object.
        3. Call the method persistify in the calculator instance to allocate the object in MictlanX
    """
    key:str = mit.nth(args, 0, "calculatorexample02")

    logger.debug("Step 1. init ActiveX object handler")
    _ = ActiveXHandler.distributed()
    
    logger.debug("Step 2. create an object instance")
    x,y = 1000,80.5 
    obj = Calculator(x=x,y=y)
    logger.debug("Step 2.1 use the calculator instance")
    result = obj.add(x,y)
    logger.debug("Step 2.1.1 Add result {} + {} = {}".format(x,y,result))
    result = obj.substract(x,y)
    logger.debug("Step 2.1.2 Substract result {} + {} = {}".format(x,y,result))
    result = obj.multiply(x,y)
    logger.debug("Step 2.1.3 Multiply result {} + {} = {}".format(x,y,result))
    result = obj.divide(x,y)
    logger.debug("Step 2.1.4 Divide result {} + {} = {}".format(x,y,result))
    # ________________________
    obj.persistify(key=key) 
    logger.debug("Step 3. Save the object ".format(x,y,result))
    
if __name__  == "__main__":
    main()
