from pymongo import MongoClient

class MongoDBConnection:
    def __init__(self, user, password, host, port):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.url = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
        self.client = None

    def connect(self):
        try:
            self.client = MongoClient(self.url)
            print("Conexión exitosa a MongoDB.")
        except Exception as e:
            print(f"Error al conectar a MongoDB: {e}")
        return self.client

    def disconnect(self):
        if self.client:
            self.client.close()
            print("Conexión cerrada.")