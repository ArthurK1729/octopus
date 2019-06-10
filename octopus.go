package main

import (
	"bufio"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Envelope struct {
	message             Message
	destinationVertexID uint32
}

type Message struct {
	candidateShortestPath uint32
}

type VertexState struct {
	shortestPathSize uint32
}

type Vertex struct {
	vertexID        uint32
	state           VertexState
	active          bool
	outNeighbourIds []uint32
	voteToHalt      bool
}

func (v *Vertex) compute(message Message) {
	if v.state.shortestPathSize > message.candidateShortestPath {
		v.state.shortestPathSize = message.candidateShortestPath
		// If a vertex has updated its state, it will want to broadcast it
		v.voteToHalt = false
	}
}

func (v *Vertex) broadcast(outboxChannel chan Envelope) {
	log.Println("Broadcasting for", v.vertexID)

	for _, nbdID := range v.outNeighbourIds {
		newState := v.state.shortestPathSize + 1
		outboxChannel <- Envelope{destinationVertexID: nbdID, message: Message{candidateShortestPath: newState}}
		// As soon as the vertex has broadcasted its new state, it will want to stop
		v.voteToHalt = true
	}

}

func inboxWorker(vertexStore map[uint32]*Vertex, inboxChannel chan Envelope) {
	log.Println("Inbox worker spawned")
	defer wg.Done()
	select {
	case envelope := <-inboxChannel:
		log.Println("Processing message for", envelope.destinationVertexID)
		v := vertexStore[envelope.destinationVertexID]
		v.compute(envelope.message)
		log.Println("State of", envelope.destinationVertexID, "now is", v.state)
	case <-time.After(1 * time.Second):
		log.Println("WORKER TIMED OUT")
		return
	}
}

// func outboxWorker(vertexStore map[uint32]*Vertex, outboxChannel chan Envelope) {
// 	log.Println("Worker spawned")
// 	for envelope := range inboxChannel {
// 		log.Println("Processing message for", envelope.destinationVertexID)
// 		v := vertexStore[envelope.destinationVertexID]
// 		v.compute(envelope.message)
// 		log.Println("State of", envelope.destinationVertexID, "now is", v.state)
// 	}
// }

func outboxWorker(vertexStore map[uint32]*Vertex, outboxChannel chan Envelope, inboxChannel chan Envelope) {
	log.Println("Executing outboxWorker")
	for _, v := range vertexStore {
		if v.state.shortestPathSize != math.MaxUint32 && v.voteToHalt == false {
			v.broadcast(outboxChannel)
		}
	}

	select {
	case envelope := <-outboxChannel:
		log.Println(envelope, "sent to inboxChannel")
		inboxChannel <- envelope
	case <-time.After(2 * time.Second):
		log.Println("OUTBOX WORKER TIMED OUT")
		return
	}
}

func displayFinalResults(vertexStore map[uint32]*Vertex) {
	for _, v := range vertexStore {
		log.Println("ID:", v.vertexID, "STATE:", v.state)
	}
}

// func readAdjlist(vertexStore map[uint32]*Vertex, path string) {
// 	file, err := os.Open(path)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		fmt.Println(scanner.Text())
// 	}

// 	if err := scanner.Err(); err != nil {
// 		log.Fatal(err)
// 	}
// }

// Reads in an adjlist and populates vertexStore
func readAdjlist(vertexStore map[uint32]*Vertex, path string) (err error) {
	log.Println("Reading", path)

	file, err := os.Open(path)
	defer file.Close()

	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		nodes := strings.Split(line, " ")
		log.Println("Loading node tuple: ", nodes)
		originNode, _ := strconv.Atoi(strings.TrimSpace(nodes[0]))
		targetNodes := make([]uint32, len(nodes[1:]))

		for index, node := range nodes[1:] {
			n, _ := strconv.Atoi(strings.TrimSpace(node))
			targetNodes[index] = uint32(n)
		}

		vertexStore[uint32(originNode)] = &Vertex{
			vertexID:        uint32(originNode),
			state:           VertexState{shortestPathSize: math.MaxUint32},
			outNeighbourIds: targetNodes,
			voteToHalt:      false,
		}

	}

	if err != io.EOF {
		log.Printf("Failed to read graph: %v\n", err)
	}

	return
}

func countUnvisited(vertexStore map[uint32]*Vertex) {
	count := 0
	for _, v := range vertexStore {
		if v.state.shortestPathSize == math.MaxUint32 {
			count++
		}
	}
	log.Println("Number of unvisited:", count)
}

// TODO:
// Create graph_utils package with a graph loader (from edgeset etc)
// go routinize vertex execution
// Figure out the unexported names thing
// Fix broadcasting problem (only should be done for active nodes)
// Fix issue where node 3 only gets the guaranteed correct value after 5 steps instead of 3
// Intelligent partitioning scheme. Don't load the whole graph into memory, rather
// read lines one by one and ship them to other nodes once buffer limit is reached.
// voteToHalt might not be threadsafe
// Verify results with single source shortest path in NetworkX
var wg sync.WaitGroup

func main() {
	log.Println("Initializing")
	var vertexStore = make(map[uint32]*Vertex)
	var inboxChannel = make(chan Envelope, 100000)
	var outboxChannel = make(chan Envelope, 100000)

	log.Println("Ingesting vertices")
	readAdjlist(vertexStore, "facebook_social_graph.adjlist")
	// readAdjlist(vertexStore, "graph.adjlist")

	log.Println("Sending seed")
	inboxChannel <- Envelope{destinationVertexID: 0, message: Message{candidateShortestPath: 0}}

	for i := 0; i < 4*len(vertexStore); i++ {
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Wait()
		outboxWorker(vertexStore, outboxChannel, inboxChannel)
		log.Println("ROUND OVER")
	}

	displayFinalResults(vertexStore)
	countUnvisited(vertexStore)
}
