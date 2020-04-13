.PHONY = init-local build-grpc-clients install build

init-local:
	brew install protobuf

build-grpc-clients:
	protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto

build: build-grpc-clients
	go mod tidy
	go build

install: build
	go install

clean:
	go clean
	rm -rf vendor
	rm -rf *.pb.go
	rm -rf *.out