# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: query.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import query_identifier_pb2 as query__identifier__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='query.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x0bquery.proto\x1a\x16query_identifier.proto\"N\n\x05Query\x12\x12\n\nquery_type\x18\x01 \x01(\t\x12\"\n\x08query_id\x18\x02 \x01(\x0b\x32\x10.QueryIdentifier\x12\r\n\x05query\x18\x03 \x01(\tb\x06proto3')
  ,
  dependencies=[query__identifier__pb2.DESCRIPTOR,])




_QUERY = _descriptor.Descriptor(
  name='Query',
  full_name='Query',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='query_type', full_name='Query.query_type', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='query_id', full_name='Query.query_id', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='query', full_name='Query.query', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
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
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=39,
  serialized_end=117,
)

_QUERY.fields_by_name['query_id'].message_type = query__identifier__pb2._QUERYIDENTIFIER
DESCRIPTOR.message_types_by_name['Query'] = _QUERY
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Query = _reflection.GeneratedProtocolMessageType('Query', (_message.Message,), dict(
  DESCRIPTOR = _QUERY,
  __module__ = 'query_pb2'
  # @@protoc_insertion_point(class_scope:Query)
  ))
_sym_db.RegisterMessage(Query)


# @@protoc_insertion_point(module_scope)