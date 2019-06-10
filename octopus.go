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

	for envelope := range inboxChannel {
		log.Println("Processing message for", envelope.destinationVertexID)
		v := vertexStore[envelope.destinationVertexID]
		v.compute(envelope.message)
		log.Println("State of", envelope.destinationVertexID, "now is", v.state)
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

	close(outboxChannel)

	for envelope := range outboxChannel {
		log.Println(envelope, "sent to inboxChannel")
		inboxChannel <- envelope
	}

	close(inboxChannel)
}

func displayFinalResults(vertexStore map[uint32]*Vertex) {
	for _, v := range vertexStore {
		log.Println("ID:", v.vertexID, "STATE:", v.state)
	}
}

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
// Split into packages
// What do I do with nodes from other connected components that will never be touched? Their voteToHalt will remain false.
var wg sync.WaitGroup

func main() {
	log.Println("Initializing")
	var vertexStore = make(map[uint32]*Vertex)
	var inboxChannel chan Envelope
	var outboxChannel chan Envelope

	log.Println("Ingesting vertices")
	readAdjlist(vertexStore, "facebook_social_graph.adjlist")
	// readAdjlist(vertexStore, "graph.adjlist")

	log.Println("Sending seed")

	for i := 0; i < 10; i++ {
		outboxChannel = make(chan Envelope, 100000)

		if i == 0 {
			inboxChannel = make(chan Envelope, 100000)
			inboxChannel <- Envelope{destinationVertexID: 0, message: Message{candidateShortestPath: 0}}
			close(inboxChannel)
		}

		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Wait()

		inboxChannel = make(chan Envelope, 100000)
		outboxWorker(vertexStore, outboxChannel, inboxChannel)
		log.Println("ROUND OVER")
	}

	displayFinalResults(vertexStore)
	countUnvisited(vertexStore)
	log.Println(vertexStore[42])
}
