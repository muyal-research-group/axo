from axo import Axo,axo_method

class Dog(Axo):

    def __init__(self, name:str,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.name = name

    @axo_method
    def bark(self,name: str=""):
        return f"{self.name}: Woof Woof to {name}"
    