# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: svc_profiles.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import query_pb2 as query__pb2
import query_identifier_pb2 as query__identifier__pb2
import profiled_query_pb2 as profiled__query__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='svc_profiles.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x12svc_profiles.proto\x1a\x0bquery.proto\x1a\x16query_identifier.proto\x1a\x14profiled_query.proto2n\n\x08Profiles\x12\x30\n\nputProfile\x12\x0e.ProfiledQuery\x1a\x10.QueryIdentifier\"\x00\x12\x30\n\ngetProfile\x12\x10.QueryIdentifier\x1a\x0e.ProfiledQuery\"\x00\x62\x06proto3')
  ,
  dependencies=[query__pb2.DESCRIPTOR,query__identifier__pb2.DESCRIPTOR,profiled__query__pb2.DESCRIPTOR,])



_sym_db.RegisterFileDescriptor(DESCRIPTOR)



_PROFILES = _descriptor.ServiceDescriptor(
  name='Profiles',
  full_name='Profiles',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=81,
  serialized_end=191,
  methods=[
  _descriptor.MethodDescriptor(
    name='putProfile',
    full_name='Profiles.putProfile',
    index=0,
    containing_service=None,
    input_type=profiled__query__pb2._PROFILEDQUERY,
    output_type=query__identifier__pb2._QUERYIDENTIFIER,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='getProfile',
    full_name='Profiles.getProfile',
    index=1,
    containing_service=None,
    input_type=query__identifier__pb2._QUERYIDENTIFIER,
    output_type=profiled__query__pb2._PROFILEDQUERY,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_PROFILES)

DESCRIPTOR.services_by_name['Profiles'] = _PROFILES

# @@protoc_insertion_point(module_scope)
