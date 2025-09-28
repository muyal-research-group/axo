import pytest
from axo import Axo
from axo.contextmanager import AxoContextManager
from .objects import Dog
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.services import MictlanXStorageService,StorageService

@pytest.fixture()
def dem():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id  = "axo-endpoint-0",
        hostname     = "localhost",
        protocol     = "tcp",
        req_res_port = 16667,
        pubsub_port  = 16666
    )
    return dem

@pytest.fixture
def storage_service() -> StorageService:
    return MictlanXStorageService()


def test_extend_dependencies():
    dog = Dog("dependency")
    expected_dependencies = ["A","B"]
    dog.extend_dependencies("A","B")

    assert dog.get_dependencies() == expected_dependencies

def test_append_dependency():
    dog = Dog("dependency")
    expected_dependencies = ["A","B"]
    dog.append_dependency("A")
    dog.append_dependency("B")

    assert dog.get_dependencies() == expected_dependencies



def test_create_instance_ao():
    name = "Rex"
    dog = Dog(name=name)
    assert isinstance(dog,Axo)
    assert isinstance(dog,Dog)
    assert dog.name == name

def test_update_instance_ao_metadata():
    name                 = "Rex"
    axo_bucket_id        = "b1"
    axo_key              = "key"
    axo_sink_bucket_id   = "b2"
    axo_source_bucket_id = "b3"
    axo_endpoint_id      = "e1"
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
    assert axo_endpoint_id == dog.get_endpoint_id()
    
    dog.set_sink_bucket_id(sink_bucket_id=axo_sink_bucket_id +"x")
    assert dog.get_axo_sink_bucket_id() == f"{axo_sink_bucket_id}x"

    dog.set_source_bucket_id(source_bucket_id=axo_source_bucket_id +"x")
    assert dog.get_axo_source_bucket_id() == f"{axo_source_bucket_id}x"

def test_ao_to_stream():
    dog = Dog(name="Rory")
    res = dog.to_stream()
    for i in res:
        assert isinstance(i, bytes)
    try:
        res = dog.to_stream(chunk_size=1)
        xs = list(res)
        assert isinstance(xs,list)
    except Exception as e:
        assert isinstance(e, Exception)

def test_get_parts():
    dog           = Dog(name="Rory")
    dog_bytes_res = dog.to_bytes()
    assert dog_bytes_res.is_ok

    res_parts     = Axo.get_parts(dog_bytes_res.unwrap())
    assert res_parts.is_ok
    raw_parts = dog.get_raw_parts()
    assert raw_parts.is_ok


@pytest.mark.asyncio
async def test_ao_persistity_no_rt():
    dog       = Dog(name="Rory")
    res = await dog.persistify()
    assert res.is_err 




def test_ao_set_endpoint():
    dog = Dog(name="Tex")
    eid = "e0"
    dog.set_endpoint_id(endpoint_id=eid)
    assert eid == dog.get_endpoint_id()



def test_ao_call():
    with AxoContextManager.local() as lrt:
        dog_name = "Rory"
        dog = Dog(name=dog_name,axo_endpoint_id="axo-endpoint-0")
        other_dog_name = "REX"
        res = Axo.call(instance=dog, method_name="bark",name=other_dog_name)
        assert res.is_ok
        response = res.unwrap()
        assert response == f"{dog_name}: Woof Woof to {other_dog_name}"
        res = Axo.call(instance=dog, method_name="name")
        assert res.is_ok 
        assert res.unwrap() == dog_name
        res = Axo.call(instance=dog, method_name="not_found_method",name=other_dog_name)
        assert res.is_err


def test_ao_to_bytes():
    dog = Dog(name="Tex")
    dog_bytes_res = dog.to_bytes()
    assert dog_bytes_res.is_ok

    assert isinstance(dog_bytes_res.unwrap(),bytes)


def test_ao_from_bytes():
    dog = Dog(name="Tex")
    dog_bytes_res = dog.to_bytes()
    assert dog_bytes_res.is_ok
    dog_bytes = dog_bytes_res.unwrap()

    the_same_dog_result= Axo.from_bytes(raw=dog_bytes)
    assert the_same_dog_result.is_ok
    the_same_dog = the_same_dog_result.unwrap()
    assert the_same_dog.name == dog.name


@pytest.skip(reason="Needs Axo only to validate Cryptomesh AO serialization")
@pytest.mark.asyncio
async def test_get_ao(dem, storage_service):
    with AxoContextManager.distributed(storage_service=storage_service,endpoint_manager=dem) as lrt:
        ao1 = await Axo.get_by_key(bucket_id="7f5e5a62bcc24d4eb3398d508670f79b",key="exampleee")
        assert ao1.is_ok
        ao1_instance:Axo = ao1.unwrap()
        
        ao1_instance.set_endpoint_id("axo-endpoint-0")
        ao1_instance.set_source_bucket_id("7f5e5a62bcc24d4eb3398d508670f79b")
        ao1_instance.set_sink_bucket_id("salida")

        res = ao1_instance.zip(source=b"")
        assert res.is_ok
        print(res.unwrap())