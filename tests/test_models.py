from axo.models import AxoRequestEnvelope,AxoReplyMsg,AxoReplyEnvelope,AxoRequestMsg
from axo.core.models import MetadataX
import pytest


def test_axo_request_envelop():
    are = AxoRequestEnvelope.model_validate({
        "allow_stale": True,
        "axo_bucket_id": "b1",
        "axo_endpoint_id": "e9",
        "axo_key": "key",
        "axo_sink_bucket_id": "sb1",
        "axo_source_bucket_id": "sob1",
        "dependencies": [],
        "class_name": "Calculator",  # Required for axo_uri generation
        "method": "sum",
        "operation": "PING",
        "pre_version": 0,
        "separator": ";",
        "task_id": None
    })

    expected_uri = f"axo://{are.axo_bucket_id}:{are.axo_key}/{are.pre_version}?class={are.class_name}&method={are.method}"
    assert are.axo_uri == expected_uri

def test_init_metadatax():
    axo_key = "ao"
    

    m = MetadataX.model_validate({
        "axo_key":axo_key,
        "axo_alias":"my_ao",
        "axo_module":"module",
        "axo_name":"AO",
        "axo_class_name":"Calc",
        "axo_version":"v0",
        "axo_bucket_id":"b1",
        "axo_source_bucket_id":"b1_source",
        "axo_sink_bucket_id":"b1_sink",
        "axo_endpoint_id":"axo_endpoint_id",
        "axo_version":0,
        "axo_dependencies":["numpy"]
    })
    assert m.axo_key == axo_key
