
import asyncio
import logging

import grpc
from src.lunch.profile_service.svc_profiles_pb2_grpc import ProfilesStub
from src.lunch.profile_service.query_pb2 import Query
from src.lunch.profile_service.query_identifier_pb2 import QueryIdentifier
from src.lunch.profile_service.profiled_query_pb2 import ProfiledQuery
from src.lunch.profile_service.profile_pb2 import Profile

async def run() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = ProfilesStub(channel)

        #
        query_id = QueryIdentifier(query_id="guf_id")

        _ = await stub.putProfile(ProfiledQuery(profile=Profile(profile="foo_profile"),
                                                       query=Query(query_type="bar_type",
                                                                   query_id=query_id,
                                                                   query="some query or other"
                                                                   )))
        profile = await stub.getProfile(query_id)

        print(profile.profile)

if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(run())