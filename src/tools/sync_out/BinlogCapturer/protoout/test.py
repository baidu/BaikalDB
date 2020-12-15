import common_pb2
from google.protobuf.service import *


RpcChannel channel = rpcImpl.Channel("xxxx:8110")
RpcController controller = rpcImpl.Controller() 
MyService service = MyService_Stub(channel) 
service.MyMethod(controller, request, callback)


ec = common_pb2.SchemaConf()
ec.need_merge = False

print ec.op_version

s = ec.SerializeToString()

print s
