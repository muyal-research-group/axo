import pytest
from axo.helpers import _build_axo_uri,_generate_id,_make_id_validator


def test_build_axo_uri():
    axo_bucket_id = "b1"
    axo_key       = "ao"
    axo_version   = 1
    class_name    = "Dog"
    method        = "bark"
    res = _build_axo_uri(
        axo_bucket_id = axo_bucket_id,
        axo_key       = axo_key,
        axo_version   = axo_version,
        class_name    = class_name,
        method        = method
    )
    assert res == f"axo://{axo_bucket_id}:{axo_key}/{axo_version}?class={class_name}&method={method}"
    res = _build_axo_uri(axo_bucket_id=axo_bucket_id,axo_key=axo_key,axo_version=axo_version)
    assert res == f"axo://{axo_bucket_id}:{axo_key}/{axo_version}"
    res = _build_axo_uri(axo_bucket_id=axo_bucket_id,axo_key=axo_key,class_name=class_name,axo_version=axo_version)
    assert res == f"axo://{axo_bucket_id}:{axo_key}/{axo_version}"
    res = _build_axo_uri(axo_bucket_id=axo_bucket_id,axo_key=axo_key,method=method,axo_version=axo_version)
    assert res == f"axo://{axo_bucket_id}:{axo_key}/{axo_version}"

def test_generate_id():
    size = 10
    res = _generate_id(val=None,size=size)
    assert len(res) == size

    val = "ID"
    expected_val = "id"
    res = _generate_id(val=val,size=size)
    assert res == expected_val
    
    val = "  ID   "
    res = _generate_id(val=val,size=size)
    assert res == expected_val


    val = "!!@#$$%@#!@@__ID____-----------__!@#!@#!$#$%$#$%$%&*  "
    res = _generate_id(val=val,size=size)
    assert res == expected_val
    
    val = "!"
    res = _generate_id(val=val,size=size)
    assert len(res) == size
