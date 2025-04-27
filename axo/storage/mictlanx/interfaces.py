from dataclasses import dataclass
@dataclass
class GetKey:
    length:int = 64
@dataclass
class PutPath(object):
    pass
    # def __init__(self,path:str):
        # self.path = path

