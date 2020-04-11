

.PHONY = init

init-local:
	brew install protobuf
	protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto
	go install