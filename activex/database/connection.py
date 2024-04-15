from pymongo import MongoClient
class Connection:
    def __init__(self, user, password, host, port):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.url = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
        self.__conn = None

    def connect(self):
        try:
            self.__conn = MongoClient(self.url)
            print("Conexión exitosa a MongoDB.")
        except Exception as e:
            print(f"Error al conectar a MongoDB: {e}")
        return self.__conn

    def disconnect(self):
        if self.__conn:
            self.__conn.close()
            print("Conexión cerrada.")