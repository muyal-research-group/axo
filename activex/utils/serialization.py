import cloudpickle

class MyObject:
    def __init__(self, value):
        self.value = value
    
    def add(self, x):
        return self.value + x

# Create an instance of MyObject
obj = MyObject(10)
# Serialize the object
serialized_obj = cloudpickle.dumps(obj)
print(serialized_obj)
deserialized_obj = cloudpickle.loads(serialized_obj)
print(deserialized_obj)
print("VALUE",deserialized_obj.value)
print("ADD_METHOD",deserialized_obj.add(10))