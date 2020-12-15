#-*- coding:utf8 -*-
from google.protobuf.descriptor_pb2 import FileDescriptorProto,DescriptorProto
from google.protobuf.message_factory import MessageFactory
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.descriptor import MakeDescriptor,FieldDescriptor
from google.protobuf.reflection import ParseMessage,MakeClass
from google.protobuf.symbol_database import SymbolDatabase,Default
from google.protobuf import json_format
import base64
import json
import time
import datetime

class DynamicMessageParser:
    mysqlType2ProtoType = {
        "INT8":FieldDescriptor.TYPE_SINT32,
        "INT16":FieldDescriptor.TYPE_SINT32,
        "INT32":FieldDescriptor.TYPE_SINT32,
        "INT64":FieldDescriptor.TYPE_SINT64,
        "UINT8":FieldDescriptor.TYPE_UINT32,
        "UINT16":FieldDescriptor.TYPE_UINT32,
        "UINT32":FieldDescriptor.TYPE_UINT32,
        "UINT64":FieldDescriptor.TYPE_UINT32,
        "FLOAT":FieldDescriptor.TYPE_FLOAT,
        "DOUBLE":FieldDescriptor.TYPE_DOUBLE,
        "STRING":FieldDescriptor.TYPE_BYTES,
        "DATETIME":FieldDescriptor.TYPE_FIXED64,
        "TIMESTAMP":FieldDescriptor.TYPE_FIXED32,
        "DATE":FieldDescriptor.TYPE_FIXED32,
        "TIME":FieldDescriptor.TYPE_SFIXED32,
        "HLL":FieldDescriptor.TYPE_BYTES,
        "BOOL":FieldDescriptor.TYPE_BOOL
    }
    def __init__(self):
        self.tableDescriptor = {}
        self.tableMaxFieldId = {}
        self.factory = Default()
        self.tableSchemaVersionSet = set()
        self.tableFieldTypeDict = {}
    def addTable(self,schema):
        descriptorProto = DescriptorProto()
        tableIDVersion = str(schema['table_id']) + '_' + str(schema['version'])
        if tableIDVersion in self.tableSchemaVersionSet:
            return
        self.tableSchemaVersionSet.add(tableIDVersion)
        tmpName = schema['namespace_name'] + '_' + schema['database'] + '_' + schema['table_name'] + '_' + str(schema['version'])
        descriptorProto.name = str(tmpName)
        self.tableMaxFieldId[schema['table_id']] = 0
        fieldTypeDict = {}
        for field in schema['fields']:
            if 'deleted' in field and field['deleted'] == True:
                continue
            fieldProto = descriptorProto.field.add()
            fieldProto.number = field['field_id']
            if fieldProto.number > self.tableMaxFieldId[schema['table_id']]:
                self.tableMaxFieldId[schema['table_id']] = fieldProto.number
            fieldProto.name = field['field_name']
            fieldProto.type = DynamicMessageParser.mysqlType2ProtoType[field['mysql_type']]
            fieldTypeDict[fieldProto.number] = field['mysql_type']
            if 'default_value' in field:
                if field['mysql_type'] in ('DATETIME','DATE','TIME','TIMESTAMP'):
                    continue
                fieldProto.default_value = base64.b64decode(field['default_value'])
        descriptor = MakeDescriptor(descriptorProto)
        self.tableDescriptor[schema['table_id']] = descriptor
        self.tableFieldTypeDict[schema['table_id']] = fieldTypeDict
        self.factory.RegisterMessageDescriptor(descriptor)
        self.factory.pool.AddDescriptor(descriptor) 
    def parseFromString(self, tableID, protoStr):
        if tableID not in self.tableDescriptor:
            print "table id not found:", tableID
            return 1, None
        protoClass = self.factory.GetPrototype(self.tableDescriptor[tableID])
        msg = protoClass()
        msg.ParseFromString(protoStr)
        ss = msg.SerializeToString()
        maxUnknownFieldId = 0
        for field in msg.UnknownFields():
            if maxUnknownFieldId < field.field_number:
                maxUnknownFieldId = field.field_number
        if maxUnknownFieldId > self.tableMaxFieldId[tableID]:
            #有新增字段，需要重新更新schema。
            print "有新增字段，需要重新更新schema"
            return 1, None
        return 0, self.pb2dict(tableID, msg)
    def datetime2str(self, datetime):
        year_month = ((datetime >> 46) & 0x1FFFF)
        year = year_month / 13
        month = year_month % 13
        day = ((datetime >> 41) & 0x1F);
        hour = ((datetime >> 36) & 0x1F);
        minute = ((datetime >> 30) & 0x3F);
        second = ((datetime >> 24) & 0x3F);

        res = "%4d-%02d-%02d %02d:%02d:%02d" % (year, month, day, hour, minute, second)
        return res
    def time2str(self, time):
        minus = False
        if(time < 0):
            minus = True
            time = -time
        hour = (time >> 12) & 0x3FF;
        minu = (time >> 6) & 0x3F;
        sec = time & 0x3F;
        res = ''
        if minus:
            res += '-'
        res += '%02d:%02d:%02d' % (hour, minu, sec)
        return res

    def timestamp2str(self, timestamp):
        res = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        return res

    def date2str(self, date):
        year_month = ((date >> 5) & 0x1FFFF)
        year = year_month / 13
        month = year_month % 13
        day = date & 0x1F
        res = "%04d-%02d-%02d" % (year, month, day)
        return res

    def pb2dict(self, tableID, obj):
        """
        Takes a ProtoBuf Message obj and convertes it to a dict.
        """
        adict = {}
        if not obj.IsInitialized():
            return None
        for field in obj.DESCRIPTOR.fields:
            value = None
            if not getattr(obj, field.name):
                if field.has_default_value:
                    value = field.default_value
                else:
                    continue
            value = getattr(obj, field.name)
            mysqlType = self.tableFieldTypeDict[tableID][field.number]
            if mysqlType == 'DATETIME':
                value = self.datetime2str(value)
            elif mysqlType == 'DATE':
                value = self.date2str(value)
            elif mysqlType == 'TIME':
                value = self.time2str(value)
            elif mysqlType == 'TIMESTAMP':
                value = self.timestamp2str(value)
            adict[field.name] = value
            if type(adict[field.name]) == long:
                adict[field.name] = int(adict[field.name])
        return adict
