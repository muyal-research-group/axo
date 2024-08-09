# Axo - Test suite
This is a simple test suite for axo, it has 4 tests:

1. Create and save an active object.
2. Create and save N active objects.
3. Consume active objects.
4. Cipher example

## Getting started

First you must deploy the following services:

1. To deploy a storage pool of ```MictlanX``` you must execute the next command:

```bash
docker compose -f ./docker/mictlanx/router.yml -p mictlanx up -d
```

2. Also you need a open telemery services, execute the following command:
```bash
docker compose -f ./docker/open_telemetry/opentelemetry.yml -p opentelemetry up -d 
```

3. You must deploy an endpoint 
```
```

## Test 1: Create and save an active object
In this example we create and save an active object in a distributed exeution environment. 

1. First you need to define the environment variables:
```py
# Unique identifier of the endpoint
AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
# Endpoint's network configuration
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
```

2. You should create an endpoint manager instance, we use our implementation of the xolo endpoint manager:

```py
endpoint_manager = XoloEndpointManager()
# Add a new endpoint
endpoint_manager.add_endpoint(
    endpoint_id  = AXO_ENDPOINT_ID,
    protocol     = AXO_ENDPOINT_PROTOCOL,
    hostname     = AXO_ENDPOINT_HOSTNAME,
    req_res_port = AXO_ENDPOINT_REQ_RES_PORT,
    pubsub_port  = AXO_ENDPOINT_PUBSUB_PORT
)
```

3. You must create an **distributed** context manager: 
```py
ax = ActiveXContextManager.distributed(
    endpoint_manager = endpoint_manager
)
```

4. Now we are ready to create instance of an active object: 
```py
rex = Dog(name="Rex")
res = rex.persistify()
```

## Test 2: Create N active objects
