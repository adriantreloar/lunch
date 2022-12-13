# -I Specify the directory in which to search for imports
# --python_out  Generate Python source file.
# --grpc_python_out  Generate Python source file.
python -m grpc_tools.protoc -I../lunch/profile_service/protos --python_out=../lunch/profile_service --grpc_python_out=../lunch/profile_service ../lunch/profile_service/protos/profile.proto
python -m grpc_tools.protoc -I../lunch/profile_service/protos --python_out=../lunch/profile_service --grpc_python_out=../lunch/profile_service ../lunch/profile_service/protos/profiled_query.proto
python -m grpc_tools.protoc -I../lunch/profile_service/protos --python_out=../lunch/profile_service --grpc_python_out=../lunch/profile_service ../lunch/profile_service/protos/query.proto
python -m grpc_tools.protoc -I../lunch/profile_service/protos --python_out=../lunch/profile_service --grpc_python_out=../lunch/profile_service ../lunch/profile_service/protos/query_identifier.proto
python -m grpc_tools.protoc -I../lunch/profile_service/protos --python_out=../lunch/profile_service --grpc_python_out=../lunch/profile_service ../lunch/profile_service/protos/svc_profiles.proto
