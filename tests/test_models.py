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
        "axo_class_name": "Calculator",  # Required for axo_uri generation
        "method": "sum",
        "operation": "PING",
        "axo_version": 0,
        "task_id": None
    })

    expected_uri = f"axo://{are.axo_bucket_id}:{are.axo_key}/{are.axo_version}?class={are.axo_class_name}&method={are.method}"
    assert are.axo_uri == expected_uri

def test_init_metadatax():
    axo_key = "ao"

    m = MetadataX.model_validate({
        "axo_key": axo_key,
        "axo_alias": "my_ao",
        "axo_module": "module",
        "axo_class_name": "Calc",
        "axo_version": 0,
        "axo_bucket_id": "b1",
        "axo_source_bucket_id": "b1_source",
        "axo_sink_bucket_id": "b1_sink",
        "axo_endpoint_id": "axo_endpoint_id",
        "axo_dependencies": ["numpy"],
    })

    assert m.axo_key == axo_key
    assert m.axo_alias == "my_ao"
    assert m.axo_module == "module"
    assert m.axo_class_name == "Calc"
    assert m.axo_version == 0

    expected_uri = f"axo://{m.axo_bucket_id}:{m.axo_key}/{m.axo_version}"
    assert m.axo_uri == expected_uri


def test_version_validator_rejects_negative():
    with pytest.raises(ValueError, match="axo_version must be >= 0"):
        MetadataX.model_validate({
            "axo_key": "k1",
            "axo_module": "m",
            "axo_class_name": "C",
            "axo_version": -1,
        })


def test_module_and_classname_must_be_non_empty():
    with pytest.raises(ValueError, match="field cannot be empty"):
        MetadataX.model_validate({
            "axo_key": "k1",
            "axo_module": "   ",  # only spaces
            "axo_class_name": "Cls",
            "axo_version": 1,
        })


def test_strip_spaces_in_fields():
    m = MetadataX.model_validate({
        "axo_key": "  key with spaces  ",
        "axo_bucket_id": " b 1 ",
        "axo_source_bucket_id": " b 1 s ",
        "axo_sink_bucket_id": " b 1 t ",
        "axo_module": " mod ",
        "axo_class_name": " Cls ",
        "axo_version": 1,
    })
    # spaces stripped
    assert m.axo_key == "keywithspaces"
    assert m.axo_bucket_id == "b1"
    assert m.axo_module == "mod"
    assert m.axo_class_name == "Cls"


def test_dependencies_are_unique_and_normalized():
    m = MetadataX.model_validate({
        "axo_key": "k1",
        "axo_module": "m",
        "axo_class_name": "Cls",
        "axo_version": 1,
        "axo_dependencies": ["numpy", "numpy ", "pandas"],
    })
    # deduplicated, normalized
    assert m.axo_dependencies == ["numpy", "pandas"]