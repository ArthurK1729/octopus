package main

import (
	context "context"
	"flag"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func slaveProcess(port string, masterHost string) {
	slaveServer := &slaveserver{}
	var conn *grpc.ClientConn
	var err error

	log.Println("Running in slave mode")
	// Ping master
	for {
		conn, err = grpc.Dial(masterHost, grpc.WithInsecure())
		if err != nil {
			log.Println("Cannot connect to master: %v\n Retrying...", err)
			time.Sleep(5 * time.Second)
		} else {
			log.Println("Connection with master is established at", masterHost)
			break
		}
	}
	// Register with master
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	masterClient := NewMasterClient(conn)
	slaveIdentifier, err := masterClient.RegisterSlave(ctx, &SlaveHost{SlaveHost: port})
	if err != nil {
		log.Fatal("Could not register with master")
	}
	log.Println("Registered with master")

	slaveServer.slaveIdentifier = slaveIdentifier
	log.Println("My slaveIdentifier is", slaveIdentifier.SlaveIdentifier)

	// Listen for instructions from master
	lis, err := net.Listen("tcp", ":"+port)
	log.Println("Listening to port", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterSlaveServer(s, slaveServer)
	log.Println("Waiting for master...")
	s.Serve(lis)
}

func masterProcess(distributionFactor uint32) {
	log.Println("Running in master mode")
	log.Println("Expecting", distributionFactor, "slaves")
	// Start master server
	lis, err := net.Listen("tcp", masterPort)
	log.Println("Listening to port", masterPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	masterServer := &masterserver{vertexSlaveRegistry: VertexSlaveRegistry{Registry: make(map[uint32]string)},
		slaveCount: 0,
		mut:        &sync.Mutex{},
	}

	s := grpc.NewServer()
	RegisterMasterServer(s, masterServer)
	log.Println("Listening for slave requests...")
	go s.Serve(lis)

	// Accept slave registrations
	for {
		log.Println("Waiting on connections. Current number of slave connections:", masterServer.slaveCount)

		if masterServer.slaveCount == distributionFactor {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// Connect to slaves
	masterServer.slaveConnectionStore = make(map[uint32]SlaveClientInfo)

	for slaveID, slaveHost := range masterServer.vertexSlaveRegistry.Registry {
		conn, err := grpc.Dial(slaveHost, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Could not connect to %v: %v", slaveHost, err)
		}
		defer conn.Close()

		masterServer.slaveConnectionStore[slaveID] = SlaveClientInfo{slaveHost: slaveHost,
			slaveClientConnection: NewSlaveClient(conn)}
	}

	log.Println("Done. Registered", masterServer.slaveCount, "slaves.")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Check heartbeats
	go func() {
		for {
			for _, slaveClient := range masterServer.slaveConnectionStore {
				r, err := slaveClient.slaveClientConnection.GetHeartbeat(ctx, &Empty{})
				if err != nil {
					log.Fatalf("Could not get heartbeat from %v: %v", slaveClient.slaveHost, err)
				}
				log.Printf("Got heartbeat from %v: %s", slaveClient.slaveHost, r.Timestamp)
			}
			time.Sleep(time.Second)
		}
	}()

	log.Println("Ingesting vertices")
	vertices, err := ReadAdjlist("data/facebook_social_graph.adjlist")

	if err != nil {
		log.Fatalln("Couldn't read in graph. Exiting")
	}

	// Partition graph and ship to respective slaves
	vertexShipments := make(map[uint32][]*Vertex)
	// verticesPerSlave := int(len(vertices) / int(distributionFactor))

	for i := 1; i <= int(distributionFactor); i++ {
		vertexShipments[uint32(i)] = make([]*Vertex, 0)
	}
	log.Println("Shipment slices initialized")

	log.Println("Splitting vertices by ID")
	for _, v := range vertices {
		destinationSlaveID := (v.VertexID % distributionFactor) + 1
		arr := vertexShipments[destinationSlaveID]
		arr = append(arr, v)
		vertexShipments[destinationSlaveID] = arr
	}
	log.Println("Done splitting vertices by ID")

	for slaveID, slave := range masterServer.slaveConnectionStore {
		log.Println("Sending vertex shipment to", slave.slaveHost)
		_, err := slave.slaveClientConnection.LoadGraphPartition(ctx, &Vertices{Vertices: vertexShipments[slaveID]})
		if err != nil {
			log.Fatalf("Could not load graph partition: %v", err)
		}
	}
	log.Println("Done loading graph partitions into slaves")

	// Superstep
	for _, slave := range masterServer.slaveConnectionStore {
		slave.slaveClientConnection.InitiateExecution(ctx, &Empty{})
	}

	time.Sleep(60 * time.Second)

}

var wg sync.WaitGroup

// TODO:
// Intelligent partitioning scheme. Don't load the whole graph into memory, rather
// read lines one by one and ship them to other nodes once buffer limit is reached.
// voteToHalt might not be threadsafe
// Verify results with single source shortest path in NetworkX
// What do I do with nodes from other connected components that will never be touched? Their voteToHalt will remain false.
// https://www.youtube.com/watch?v=YEKjSzIwAdA try select default too?
// Implement multi-algorithm message passing
// Use a ring hash to distribute the vertices https://godoc.org/github.com/golang/groupcache/consistenthash
// Collect resulting vertexStore from each slave. Append all results to a local file
// Figure out how to make empty calls (remove &Empty{} throughout)
// Add SlaveDelegate. Encapsulate all information for each slave (hostname, connection object, hash number) in a struct
// Implement Brandes algorithm for betweenness https://www.cl.cam.ac.uk/teaching/1617/MLRD/handbook/brandes.pdf
// Implement Pagerank as described in original pregel paper
// Clean protobuff schema
// Create config file for all the timeouts and such
// Why do vertexIDs start with 1 again?
// VertexSlaveRegistry and SlaveClientInfo should be the same thing
// Implement SlaveDone
// Implement PopulateInbox
// Refactor: create idl package
// Refactor: create config package
func main() {
	modePtr := flag.String("mode", "master", "master or slave run mode")
	slavePortPtr := flag.String("slavePort", "50052", "port for slave node")
	masterHostPtr := flag.String("masterHost", "localhost:50051", "master node hostname")
	distributionFactorPtr := flag.Uint("distributionFactor", 1, "number of slave nodes to expect")

	log.Println(*modePtr)
	flag.Parse()
	if *modePtr == "slave" {
		slaveProcess(*slavePortPtr, *masterHostPtr)
	} else if *modePtr == "master" {
		masterProcess(uint32(*distributionFactorPtr))
	}
}
