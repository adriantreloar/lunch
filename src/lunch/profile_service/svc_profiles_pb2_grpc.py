# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import profiled_query_pb2 as profiled__query__pb2
import query_identifier_pb2 as query__identifier__pb2


class ProfilesStub(object):
  """The profile service definition.
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.putProfile = channel.unary_unary(
        '/Profiles/putProfile',
        request_serializer=profiled__query__pb2.ProfiledQuery.SerializeToString,
        response_deserializer=query__identifier__pb2.QueryIdentifier.FromString,
        )
    self.getProfile = channel.unary_unary(
        '/Profiles/getProfile',
        request_serializer=query__identifier__pb2.QueryIdentifier.SerializeToString,
        response_deserializer=profiled__query__pb2.ProfiledQuery.FromString,
        )


class ProfilesServicer(object):
  """The profile service definition.
  """

  def putProfile(self, request, context):
    """Saves a profile to the central store
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def getProfile(self, request, context):
    """Requests a profile from the central store
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_ProfilesServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'putProfile': grpc.unary_unary_rpc_method_handler(
          servicer.putProfile,
          request_deserializer=profiled__query__pb2.ProfiledQuery.FromString,
          response_serializer=query__identifier__pb2.QueryIdentifier.SerializeToString,
      ),
      'getProfile': grpc.unary_unary_rpc_method_handler(
          servicer.getProfile,
          request_deserializer=query__identifier__pb2.QueryIdentifier.FromString,
          response_serializer=profiled__query__pb2.ProfiledQuery.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'Profiles', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
