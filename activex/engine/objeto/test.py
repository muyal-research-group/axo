from activeX import objectX,Client

class Perro(objectX):
    def __init__(self,nombre):
        self.nombre = nombre
        
        
if __name__ == "__main__":
    Client(host="localhost",port=8080, username="alex", password="password", dataset="my_datastore")
    p=Perro("paloma")
    p.make_persistent(alias="miperro")