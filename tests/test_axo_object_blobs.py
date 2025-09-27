import pytest
from axo.storage.types import AxoObjectBlob,AxoObjectBlobs,AxoStorageMetadata
import hashlib
import json
from axo.serde import serialize_attrs


def test_create_ao_blob_from_source_code():
    bucket_id   = "b1"
    key         = "key1"
    code        = "print('hello world')"
    producer_id = "producer1"
    source_code_key = f"{key}_source_code"
    axo_blob = AxoObjectBlob.from_source_code(
        bucket_id   = bucket_id,
        key         = key,
        code        = code,
        producer_id = producer_id
    )

    assert isinstance(axo_blob,AxoObjectBlob)
    assert axo_blob.data == code.encode('utf-8')
    assert axo_blob.metadata.content_type == "text/plain"
    assert axo_blob.metadata.ball_id == source_code_key
    assert axo_blob.metadata.bucket_id == bucket_id
    assert axo_blob.metadata.checksum == "939338e3b6ab652043d93dff9c1e8eaa69a0a969a4a0de50870a6cbad7f3c117"  # SHA256 of the code
    assert axo_blob.metadata.is_disabled == False
    assert axo_blob.metadata.key == source_code_key
    assert axo_blob.metadata.producer_id == producer_id
def test_from_source_code():
    """
    Tests the creation of an AxoObjectBlob from a source code string.
    """
    # Arrange
    bucket_id = "test-bucket"
    key = "my-object"
    code = "def main():\n    print('hello')"
    tags = {"version": "1.0"}
    
    expected_data = code.encode('utf-8')
    expected_checksum = hashlib.sha256(expected_data).hexdigest()
    expected_key = f"{key}_source_code"
    expected_size = len(expected_data)

    # Act
    blob = AxoObjectBlob.from_source_code(
        bucket_id=bucket_id,  key=key, code=code, tags=tags
    )

    # Assert
    assert isinstance(blob, AxoObjectBlob)
    assert blob.data == expected_data
    assert blob.metadata.content_type == "text/plain"
    assert blob.metadata.checksum == expected_checksum
    assert blob.metadata.key == expected_key
    assert blob.metadata.size == expected_size
    assert blob.metadata.bucket_id == bucket_id
    assert blob.metadata.tags == tags


def test_from_attrs():
    """

    Tests the creation of an AxoObjectBlob from an attributes dictionary.
    """
    # Arrange
    bucket_id = "test-bucket"
    key = "my-object-attrs"
    attrs = {"author": "axo", "retries": 3}
    
    expected_data,_ = serialize_attrs(attrs)
    expected_checksum = hashlib.sha256(expected_data).hexdigest()
    expected_key = f"{key}_attrs"   
    expected_size = len(expected_data)

    # Act
    blob = AxoObjectBlob.from_attrs(
        bucket_id=bucket_id,  key=key, attrs=attrs
    )

    # Assert
    assert isinstance(blob, AxoObjectBlob)
    # assert blob.data == expected_data
    assert blob.metadata.content_type == "application/json"
    assert blob.metadata.checksum == expected_checksum
    assert blob.metadata.key == expected_key
    assert blob.metadata.size == expected_size


def test_from_code_and_attrs():
    """
    Tests the creation of a container holding both source code and attribute blobs.
    """
    # Arrange
    bucket_id = "test-bucket"
    key = "my-combined-object"
    code = "class MyClass: pass"
    attrs = {"version": "2.0"}

    # Act
    blobs = AxoObjectBlob.from_code_and_attrs(
        bucket_id=bucket_id,  key=key, code=code, attrs=attrs
    )

    # Assert
    assert isinstance(blobs, AxoObjectBlobs)
    
    # Verify the source code blob
    src_blob = blobs.source_code_blob
    assert isinstance(src_blob, AxoObjectBlob)
    assert src_blob.metadata.content_type == "text/plain"
    assert src_blob.data == code.encode('utf-8')
    assert src_blob.metadata.key == f"{key}_source_code"
    
    # Verify the attributes blob
    expected_attrs_data,_ = serialize_attrs(attrs)
    attr_blob = blobs.attrs_blob
    assert isinstance(attr_blob, AxoObjectBlob)
    assert attr_blob.metadata.content_type == "application/json"
    assert attr_blob.data == expected_attrs_data
    assert attr_blob.metadata.key == f"{key}_attrs"