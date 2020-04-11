.PHONY = init-local init install build

init-local:
	brew install protobuf

init:
	protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto
	go install

install:
	go install

build:
	go mod tidy
	go build

clean:
	go clean
	rm -rf vendor
	rm -rf *.pb.go