from .files import is_writing
import cloudpickle as CP
import pickle as P
import humanfriendly as HF
# from activex import ActiveX
def serialize_and_yield_chunks(obj, chunk_size:str = "1mb"):
    try:
        serialized_data = obj.to_bytes()
        total_size = len(serialized_data)
        start = 0
        chunk_size = HF.parse_size(chunk_size)
        
        while start < total_size:
            end = min(start + chunk_size, total_size)
            yield serialized_data[start:end]
            start = end
    except Exception as e:
        raise e