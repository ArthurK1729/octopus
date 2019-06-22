FROM golang:1.12-alpine

WORKDIR /go/src/app
COPY . .

RUN apk add protobuf git
RUN go get -d -v ./...
RUN go install -v ./...
RUN go get -u github.com/golang/protobuf/protoc-gen-go

RUN protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto
RUN go build

ENTRYPOINT ["app"]