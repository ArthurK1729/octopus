Runbook

# How to compile protobuff
protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto