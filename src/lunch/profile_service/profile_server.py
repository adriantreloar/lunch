import asyncio
import logging
import grpc

from src.lunch.profile_service.svc_profiles_pb2_grpc import ProfilesServicer as g_ProfilesServicer

from src.lunch.profile_service.svc_profiles_pb2_grpc import add_ProfilesServicer_to_server


class ProfilesServicer(g_ProfilesServicer):

    def __init__(self):
        # TODO - this must be a time limited and space limited cache
        self.queries_cache = {}

    async def putProfile(self, request, context):
        query_id = request.query.query_id.query_id
        self.queries_cache[query_id] = request
        logging.info(f"Cached query id {query_id}")
        return request.query_id

    async def getProfile(self, request, context):
        query_id = request.query_id
        logging.info(f"Retrieving query id {query_id}")
        return self.queries_cache[query_id]


async def serve() -> None:
    server = grpc.aio.server()
    add_ProfilesServicer_to_server(ProfilesServicer(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())


