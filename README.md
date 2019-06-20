# Compile the proto files
`protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto`

# Build project
`go build`

# Create slaves
`./octopus --mode=slave --masterHost=localhost:50051 --slavePort=50052`

# Create master
`./octopus --mode=master --distributionFactor=2` 
<br/>
Master will wait for slaves to register with it such that their number is equal to `distributionFactor`. Then it kicks off the processing.