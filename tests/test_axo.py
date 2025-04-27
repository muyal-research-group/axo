import sys
sys.path.append("/home/nacho/Programming/Python/activex/tests/")
import pytest
from axo import Axo,MetadataX
from objects import Dog
from axo.runtime.local import LocalRuntime

def test_ao():
    dog = Dog(
        name = "Rex",
        axo_key = "RexPerro",
        endpoint_id = "ENDPOINT_ID",
    )
    assert dog._acx_local
    assert dog.get_axo_key() == "RexPerro"
    assert dog.get_endpoint_id() =="ENDPOINT_ID"

def test_ao_to_from_bytes():
    dog = Dog(
        name = "Rex",
        axo_key = "RexPerro",
        endpoint_id = "ENDPOINT_ID"
    )

    dog_bytes = dog.to_bytes()
    new_dog = Axo.from_bytes(raw=dog_bytes)
    assert new_dog.is_ok
    rex = new_dog.unwrap()
    assert type(rex.bark(name="ANOTHER DOG")) == str
def test_ao_persistify():
    rt = LocalRuntime(runtime_id="local")
    # rt.run()
    dog = Dog(
        name = "Rex",
        axo_key = "RexPerro",
        endpoint_id = "ENDPOINT_ID"
    )

    res = dog.persistify()
    print(res)