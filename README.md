# 🐙 Octopus 
Octopus is a Hadoop-free Pregel-based distributed graph processing engine. It is meant to be an open source and off-the-shelf solution for companies wishing to derive features from their network-based data in a scalable way. All you have to do is supply a graph, select which features you want to derive using a command line argument, and presto: you have yourself a feature vector you can load into your favourite machine learning model 🚀
# Compile the proto files
`protoc --proto_path=idl --go_out=plugins=grpc:. idl/master_data.proto idl/slave_data.proto`

# Build project
`go build`

# Create slaves
`./octopus --mode=slave --masterHost=localhost:50051 --slavePort=50052 --concurrencyLevel=4`

# Create master
`./octopus --mode=master --distributionFactor=2` 
<br/>
Master will wait for slaves to register with it such that their number is equal to `distributionFactor`. Then it kicks off the processing.
