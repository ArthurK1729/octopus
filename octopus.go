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

func slaveProcess(port string) {
	log.Println("Running in slave mode")

	lis, err := net.Listen("tcp", ":"+port)
	log.Println("Listening to port", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterSlaveServer(s, &slaveserver{})
	log.Println("Waiting for master...")
	s.Serve(lis)
}

func masterProcess() {
	log.Println("Running in master mode")
	// Start master server
	lis, err := net.Listen("tcp", masterPort)
	log.Println("Listening to port", masterPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterMasterServer(s, &masterserver{})
	log.Println("Listening for slave requests...")
	go s.Serve(lis)

	// Connect to slaves
	address1 := "localhost" + slavePort1
	address2 := "localhost" + slavePort2
	conn1, err := grpc.Dial(address1, grpc.WithInsecure())
	conn2, err := grpc.Dial(address2, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn1.Close()
	defer conn2.Close()

	c1 := NewSlaveClient(conn1)
	c2 := NewSlaveClient(conn2)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	// Continue here. Enable distribution between slaves
	defer cancel()
	// Check heartbeat
	go func() {
		for {
			r, err := c.GetHeartbeat(ctx, &Empty{})
			if err != nil {
				log.Fatalf("Could not get heartbeat: %v", err)
			}
			log.Printf("Got heartbeat: %s", r.Timestamp)
			time.Sleep(time.Second)
		}
	}()

	log.Println("Ingesting vertices")
	vertices, err := ReadAdjlist("data/facebook_social_graph.adjlist")

	if err != nil {
		log.Println("Couldn't read in graph. Exiting")
		panic("Couldn't read in graph")
	}

	vertexShipment := []*Vertex{}

	for _, v := range vertices {
		vertexShipment = append(vertexShipment, v)
	}

	c.LoadGraphPartition(ctx, &Vertices{Vertices: vertexShipment})
	if err != nil {
		log.Fatalf("Could not load graph partition: %v", err)
	}
	time.Sleep(5 * time.Second)
	c.InitiateExecution(ctx, &Empty{})

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
func main() {
	modePtr := flag.String("mode", "master", "master or slave run mode")
	portPtr := flag.String("slavePort", "50051", "port for slave node")

	log.Println(*modePtr)
	flag.Parse()
	if *modePtr == "slave" {
		slaveProcess(*portPtr)
	} else if *modePtr == "master" {
		masterProcess()
	}
}
