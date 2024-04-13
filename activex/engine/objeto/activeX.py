import socket
import cloudpickle

from metadata import objectMetadata
global __conn
__conn = None

class __conection:
    def __init__(self, host, port, username, password, dataset):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dataset = dataset
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def send(self, url, data):
        self.conn.connect((self.host, self.port))
        print(self.conn)
        self.conn.sendall(url.encode())
        response = self.conn.recv(1024)
        
    def close(self):
        self.conn.close()
        
def _get_conection():
    global __conn
    if __conn is None:
        print("No connection")
        return None
    else:
        print("connection")
        return __conn
    
    
def Client(host, port, username, password, dataset):
    global __conn
    __conn = __conection(host, port, username, password, dataset)
    

class objectX:
    _ob_meta: objectMetadata
    _ob_is_local: bool = True
    _ob_is_loaded: bool = True
    _ob_is_registered: bool = False
    _ob_is_replica: bool = False

    def __init__(self):
        pass   
    
    def make_persistent(self, alias):
        conn=_get_conection()
        self.alias = alias
        path = "/object/" + alias
        
        if conn is None:
            print("inicialice el cliente")
        else:
            print("cliente inicializado")
            if self._ob_is_registered:
                return "Objeto registrado."
            else:
                encoded_data = cloudpickle.dumps(self)
                print(encoded_data)
                print("deserializar el objeto:")
                print(cloudpickle.loads(encoded_data))
                data = {'obj': encoded_data}
            
