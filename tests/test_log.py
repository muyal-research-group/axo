import pytest
from axo.log import Log,CSVFormatter,get_logger
import os

log = get_logger(
    name  = __name__ ,
    ltype = os.environ.get("AXO_LOG_TYPE","json") ,
    debug = os.environ.get("AXO_DEBUG","1")       == "1",
    path  = os.environ.get("AXO_LOG_PATH","/log") ,
)
def test_log():
    log.info({"XXEDFDD":"SOME_VALUE","n":1,"TYOTLA":"AAAA","event":"CREATED"})

