# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: reverse.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='reverse.proto',
  package='baikaldb.pb',
  syntax='proto2',
  serialized_options=None,
  serialized_pb=_b('\n\rreverse.proto\x12\x0b\x62\x61ikaldb.pb\"\\\n\x11\x43ommonReverseNode\x12\x0b\n\x03key\x18\x01 \x01(\x0c\x12*\n\x04\x66lag\x18\x02 \x02(\x0e\x32\x1c.baikaldb.pb.ReverseNodeType\x12\x0e\n\x06weight\x18\x03 \x01(\x02\"J\n\x11\x43ommonReverseList\x12\x35\n\rreverse_nodes\x18\x01 \x03(\x0b\x32\x1e.baikaldb.pb.CommonReverseNode\"\xff\x01\n\x0eXbsReverseNode\x12\x0b\n\x03key\x18\x01 \x01(\x0c\x12*\n\x04\x66lag\x18\x02 \x02(\x0e\x32\x1c.baikaldb.pb.ReverseNodeType\x12\x35\n\x08triggers\x18\x03 \x03(\x0b\x32#.baikaldb.pb.XbsReverseNode.Trigger\x12\x0e\n\x06weight\x18\x04 \x01(\x02\x12\x0e\n\x06userid\x18\x05 \x01(\r\x12\x0e\n\x06source\x18\x06 \x01(\r\x1aM\n\x07Trigger\x12\x12\n\ntag_source\x18\x01 \x02(\r\x12\x0e\n\x06weight\x18\x02 \x01(\x02\x12\x0c\n\x04term\x18\x03 \x01(\x0c\x12\x10\n\x08query_id\x18\x04 \x01(\x04\"D\n\x0eXbsReverseList\x12\x32\n\rreverse_nodes\x18\x01 \x03(\x0b\x32\x1b.baikaldb.pb.XbsReverseNode*C\n\x0fReverseNodeType\x12\x17\n\x13REVERSE_NODE_NORMAL\x10\x00\x12\x17\n\x13REVERSE_NODE_DELETE\x10\x01')
)

_REVERSENODETYPE = _descriptor.EnumDescriptor(
  name='ReverseNodeType',
  full_name='baikaldb.pb.ReverseNodeType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='REVERSE_NODE_NORMAL', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='REVERSE_NODE_DELETE', index=1, number=1,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=528,
  serialized_end=595,
)
_sym_db.RegisterEnumDescriptor(_REVERSENODETYPE)

ReverseNodeType = enum_type_wrapper.EnumTypeWrapper(_REVERSENODETYPE)
REVERSE_NODE_NORMAL = 0
REVERSE_NODE_DELETE = 1



_COMMONREVERSENODE = _descriptor.Descriptor(
  name='CommonReverseNode',
  full_name='baikaldb.pb.CommonReverseNode',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='baikaldb.pb.CommonReverseNode.key', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='flag', full_name='baikaldb.pb.CommonReverseNode.flag', index=1,
      number=2, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='weight', full_name='baikaldb.pb.CommonReverseNode.weight', index=2,
      number=3, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=30,
  serialized_end=122,
)


_COMMONREVERSELIST = _descriptor.Descriptor(
  name='CommonReverseList',
  full_name='baikaldb.pb.CommonReverseList',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reverse_nodes', full_name='baikaldb.pb.CommonReverseList.reverse_nodes', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=124,
  serialized_end=198,
)


_XBSREVERSENODE_TRIGGER = _descriptor.Descriptor(
  name='Trigger',
  full_name='baikaldb.pb.XbsReverseNode.Trigger',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='tag_source', full_name='baikaldb.pb.XbsReverseNode.Trigger.tag_source', index=0,
      number=1, type=13, cpp_type=3, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='weight', full_name='baikaldb.pb.XbsReverseNode.Trigger.weight', index=1,
      number=2, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='term', full_name='baikaldb.pb.XbsReverseNode.Trigger.term', index=2,
      number=3, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='query_id', full_name='baikaldb.pb.XbsReverseNode.Trigger.query_id', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=379,
  serialized_end=456,
)

_XBSREVERSENODE = _descriptor.Descriptor(
  name='XbsReverseNode',
  full_name='baikaldb.pb.XbsReverseNode',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='baikaldb.pb.XbsReverseNode.key', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='flag', full_name='baikaldb.pb.XbsReverseNode.flag', index=1,
      number=2, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='triggers', full_name='baikaldb.pb.XbsReverseNode.triggers', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='weight', full_name='baikaldb.pb.XbsReverseNode.weight', index=3,
      number=4, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='userid', full_name='baikaldb.pb.XbsReverseNode.userid', index=4,
      number=5, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='source', full_name='baikaldb.pb.XbsReverseNode.source', index=5,
      number=6, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_XBSREVERSENODE_TRIGGER, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=201,
  serialized_end=456,
)


_XBSREVERSELIST = _descriptor.Descriptor(
  name='XbsReverseList',
  full_name='baikaldb.pb.XbsReverseList',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reverse_nodes', full_name='baikaldb.pb.XbsReverseList.reverse_nodes', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=458,
  serialized_end=526,
)

_COMMONREVERSENODE.fields_by_name['flag'].enum_type = _REVERSENODETYPE
_COMMONREVERSELIST.fields_by_name['reverse_nodes'].message_type = _COMMONREVERSENODE
_XBSREVERSENODE_TRIGGER.containing_type = _XBSREVERSENODE
_XBSREVERSENODE.fields_by_name['flag'].enum_type = _REVERSENODETYPE
_XBSREVERSENODE.fields_by_name['triggers'].message_type = _XBSREVERSENODE_TRIGGER
_XBSREVERSELIST.fields_by_name['reverse_nodes'].message_type = _XBSREVERSENODE
DESCRIPTOR.message_types_by_name['CommonReverseNode'] = _COMMONREVERSENODE
DESCRIPTOR.message_types_by_name['CommonReverseList'] = _COMMONREVERSELIST
DESCRIPTOR.message_types_by_name['XbsReverseNode'] = _XBSREVERSENODE
DESCRIPTOR.message_types_by_name['XbsReverseList'] = _XBSREVERSELIST
DESCRIPTOR.enum_types_by_name['ReverseNodeType'] = _REVERSENODETYPE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

CommonReverseNode = _reflection.GeneratedProtocolMessageType('CommonReverseNode', (_message.Message,), {
  'DESCRIPTOR' : _COMMONREVERSENODE,
  '__module__' : 'reverse_pb2'
  # @@protoc_insertion_point(class_scope:baikaldb.pb.CommonReverseNode)
  })
_sym_db.RegisterMessage(CommonReverseNode)

CommonReverseList = _reflection.GeneratedProtocolMessageType('CommonReverseList', (_message.Message,), {
  'DESCRIPTOR' : _COMMONREVERSELIST,
  '__module__' : 'reverse_pb2'
  # @@protoc_insertion_point(class_scope:baikaldb.pb.CommonReverseList)
  })
_sym_db.RegisterMessage(CommonReverseList)

XbsReverseNode = _reflection.GeneratedProtocolMessageType('XbsReverseNode', (_message.Message,), {

  'Trigger' : _reflection.GeneratedProtocolMessageType('Trigger', (_message.Message,), {
    'DESCRIPTOR' : _XBSREVERSENODE_TRIGGER,
    '__module__' : 'reverse_pb2'
    # @@protoc_insertion_point(class_scope:baikaldb.pb.XbsReverseNode.Trigger)
    })
  ,
  'DESCRIPTOR' : _XBSREVERSENODE,
  '__module__' : 'reverse_pb2'
  # @@protoc_insertion_point(class_scope:baikaldb.pb.XbsReverseNode)
  })
_sym_db.RegisterMessage(XbsReverseNode)
_sym_db.RegisterMessage(XbsReverseNode.Trigger)

XbsReverseList = _reflection.GeneratedProtocolMessageType('XbsReverseList', (_message.Message,), {
  'DESCRIPTOR' : _XBSREVERSELIST,
  '__module__' : 'reverse_pb2'
  # @@protoc_insertion_point(class_scope:baikaldb.pb.XbsReverseList)
  })
_sym_db.RegisterMessage(XbsReverseList)


# @@protoc_insertion_point(module_scope)
