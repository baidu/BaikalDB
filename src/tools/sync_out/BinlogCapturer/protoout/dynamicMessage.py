from google.protobuf.descriptor_pb2 import FileDescriptorProto,DescriptorProto
from google.protobuf.message_factory import MessageFactory
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.descriptor import MakeDescriptor,FieldDescriptor
from google.protobuf.reflection import ParseMessage
from google.protobuf.symbol_database import SymbolDatabase,Default


def parseFromString(pbStr):
    fdp = FileDescriptorProto()
    
    dp = fdp.message_type.add()
    dp.name = 'tb1'
    fieldList = [
        {"id":1, "name":"id", "type":FieldDescriptor.TYPE_SINT32},
#        {"id":2, "name":"name", "type":FieldDescriptor.TYPE_STRING},
        {"id":3, "name":"addr", "type":FieldDescriptor.TYPE_STRING}
    ]
    
    
    for field in fieldList:
        fieldProto = dp.field.add()
        fieldProto.number = field["id"]
        fieldProto.type = field['type']
        fieldProto.name = field['name']
    
    dtop = MakeDescriptor(dp)
    factory = Default()
    factory.pool.AddDescriptor(dtop) 
    MyProtoClass = factory.GetPrototype(dtop)
    myproto_instance = MyProtoClass()
    myproto_instance.ParseFromString(pbStr)
   # print dir(myproto_instance)
    ss = myproto_instance.SerializeToString()
    print ss == pbStr
    return myproto_instance
#    return ParseMessage(dtop, pbStr)
