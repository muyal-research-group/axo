import pytest
from axo.storage.types import AxoObjectBlob,AxoObjectBlobs,AxoStorageMetadata



def test_create_ao_blob_from_source_code():
    bucket_id   = "b1"
    ball_id     = "ball1"
    key         = "key1"
    code        = "print('hello world')"
    producer_id = "producer1"

    axo_blob = AxoObjectBlob.from_source_code(
        bucket_id   = bucket_id,
        ball_id     = ball_id,
        key         = key,
        code        = code,
        producer_id = producer_id
    )

    assert isinstance(axo_blob,AxoObjectBlob)
    assert axo_blob.data == code.encode('utf-8')
    assert axo_blob.metadata.content_type == "text/plain"
    assert axo_blob.metadata.ball_id == f"{ball_id}_source_code"
    assert axo_blob.metadata.bucket_id == bucket_id
    assert axo_blob.metadata.checksum == "939338e3b6ab652043d93dff9c1e8eaa69a0a969a4a0de50870a6cbad7f3c117"  # SHA256 of the code
    assert axo_blob.metadata.is_disabled == False
    assert axo_blob.metadata.key == key
    assert axo_blob.metadata.producer_id == producer_id
