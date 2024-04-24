import zmq 
import time
import os 

import logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.setsockopt(zmq.SUBSCRIBE,b"activex")
# socket.setsockopt_string(zmq.SUBSCRIBE,"")

protocol =  os.environ.get("ACTIVEX_MIDDLEWARE_PROTOCOL","tcp")
port     = int(os.environ.get("ACTIVEX_MIDDLEWARE_PORT",60667))
hostname = os.environ.get("ACTIVE_MIDDLEWARE_HOSTNAME","127.0.0.1")
uri =  hostname if port == -1 else "{}:{}".format(hostname,port)

socket.connect("{}://{}".format(protocol,uri))

# socket.subscribe(b"")

def main():
    logger.debug("Listen on {}://{}".format(protocol,uri))
    try:
        while True: 
            msg = socket.recv_string()
            logger.debug("Received request: %s",msg)
            #  Do some 'work'
            time.sleep(1)
    except Exception as e:
        logger.error(e)

        #  Send reply back to client
        # socket.send(b"World")
if __name__ == "__main__":
    main()