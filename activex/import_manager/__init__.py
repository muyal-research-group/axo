import sys
import os
import types
from typing import Any
from abc import ABC,abstractmethod

class ImportManager(ABC):
    @abstractmethod
    def add_module(self, module:str,class_name:str, classs:Any):
        pass
class DefaultImportManager(ImportManager):
    def __init__(self):
        pass
    def add_module(self, module:str,class_name:str, classs:Any):
        if module not in sys.modules:
            new_module = types.ModuleType(module)
            sys.modules[module] = new_module
        sys.modules[module][class_name] = classs