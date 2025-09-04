import pytest
from axo.log import Log,CSVFormatter,get_logger

log = get_logger(name="TESTR")
def test_log():
    log.info({"XXEDFDD":"SOME_VALUE","n":1,"TYOTLA":"AAAA","event":"CREATED"})

