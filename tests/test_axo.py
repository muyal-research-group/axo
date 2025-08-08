import pytest
from axo import Axo,MetadataX
from .objects import Dog
from axo.contextmanager import AxoContextManager



def test_create_instance_ao():
    name = "Rex"
    dog = Dog(name=name)
    assert isinstance(dog,Axo)
    assert isinstance(dog,Dog)
    assert dog.name == name

def test_update_instance_ao_metadata():
    name = "Rex"
    axo_bucket_id ="b1"
    axo_key = "key"
    axo_sink_bucket_id = "b2"
    axo_source_bucket_id = "b3"
    axo_endpoint_id = "e1"
    dog = Dog(
        name=name,
        axo_bucket_id=axo_bucket_id,
        axo_key =axo_key,
        axo_sink_bucket_id = axo_sink_bucket_id,
        axo_source_bucket_id = axo_source_bucket_id,
        axo_endpoint_id = axo_endpoint_id
    )
    assert axo_bucket_id == dog.get_axo_bucket_id()
    assert axo_key == dog.get_axo_key()
    assert axo_source_bucket_id == dog.get_axo_source_bucket_id()
    assert axo_sink_bucket_id == dog.get_axo_sink_bucket_id()
    assert axo_endpoint_id == axo_endpoint_id


def test_ao_to_bytes():
    dog = Dog(name="Tex")
    dog_bytes = dog.to_bytes()
    assert isinstance(dog_bytes,bytes)


def test_ao_from_bytes():
    dog = Dog(name="Tex")
    dog_bytes = dog.to_bytes()
    the_same_dog_result= Axo.from_bytes(raw=dog_bytes)
    assert the_same_dog_result.is_ok
    the_same_dog = the_same_dog_result.unwrap()
    assert the_same_dog.name == dog.name
