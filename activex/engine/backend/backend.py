import socket
import sys
import signal
from threading import Thread
from config import configDataBase
from manager import MongoDBConnection

import api
class DateStore:

    def __init__(self, host, port, user,password, database):
        _conection = MongoDBConnection(user=configDataBase["user"],password=configDataBase["password"],host=configDataBase["host"],port=configDataBase["port"])
        self._client = _conection.connect()
        self._db = self._client.client[database]
        self._coleccion = self._db[user]
        self._host = host
        self._port = port
        self._server = None
        
    def client_manager(self, client, ip):
        pass

    def start(self):
        api.start_app(self)