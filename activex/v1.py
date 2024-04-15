# Stuff
from typing import Dict
import cloudpickle as CP
from activex.decorators import activex
from nanoid import generate as nanoid 
import string
from abc import ABC

ALPHABET = string.ascii_lowercase+string.digits

class ActiveX(ABC):
    def __init__(self,
                 object_id:str, 
                 tags:Dict[str,str]={}
    ):     
        self.object_id = nanoid(ALPHABET) if object_id =="" else object_id
    
    def to_bytes(self):
        return CP.dumps(self)

    @staticmethod
    def from_bytes(raw_obj:bytes):
        return CP.loads(raw_obj)
    
    def save(self):
        pass


class Dog(ActiveX):
    def __init__(self, object_id: str="", tags: Dict[str, str]={}):
        super().__init__(object_id, tags)
    # local or distributed
    @activex
    def bark(self):
        print("WOOF")


if __name__ == "__main__":
    d = Dog(object_id="")
    d.bark()
    
    obj_bytes = d.to_bytes()

    print("BYTES",obj_bytes)
    obj = ActiveX.from_bytes(obj_bytes)
    print("OBJ",obj)
    obj.bark()
    print("OBJECT_ID",obj.object_id)
