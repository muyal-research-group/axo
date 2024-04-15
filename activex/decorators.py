
from functools import wraps

def activex(f):
    @wraps(f)
    def __activex(*args,**kwargs):

        print("BEFORE EXECUTION")
        print("_"*10)
        result = f(*args,**kwargs)
        print("_"*10)
        print("AFTER EXCUTION")
        return result
    return __activex
