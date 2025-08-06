import sys
sys.path.append("/home/nacho/Programming/Python/activex/tests/")
import pytest
from axo import Axo,MetadataX
from objects import Dog
from axo.contextmanager import AxoContextManager
def test_ao():
    dog = Dog(
        name = "Rex",
        axo_key = "RexPerro",
        axo_endpoint_id = "ENDPOINT_ID",
    )
    assert dog._acx_local
    assert dog.get_axo_key() == "RexPerro"
    # assert dog.get_endpoint_id() =="ENDPOINT_ID"
    print(dog.__dict__)

def test_ao_to_from_bytes():
    with AxoContextManager.local() as lcm:
        dog = Dog(
            name = "Rex",
            axo_key = "RexPerro",
            endpoint_id = "ENDPOINT_ID"
        )

        dog_bytes = dog.to_bytes()
        new_dog = Axo.from_bytes(raw=dog_bytes)
        assert new_dog.is_ok
        rex = new_dog.unwrap()
        bark_result = rex.bark(name="ANOTHER DOG")
        assert bark_result.is_ok
        # assert type( ) == str

@pytest.mark.asyncio
async def test_ao_persistify():
    # rt = LocalRuntime(storage_service=LocalStorageService(storage_service_id="local"),runtime_id="local")
    # rt.run()
    with AxoContextManager.local() as lcm:
        dog = Dog(
            name = "Rex",
            axo_key = "RexPerro",
            axo_endpoint_id = "ENDPOINT_ID"
        )

        res = await dog.persistify()
        print(res)