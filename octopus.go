package main

import (
	"log"
	"math"
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
}

func (v *Vertex) compute(message Message) {
	if v.state.shortestPathSize > message.candidateShortestPath {
		v.state.shortestPathSize = message.candidateShortestPath
	}
}

func (v *Vertex) broadcast(outboxChannel chan Envelope) {
	for _, nbdID := range v.outNeighbourIds {
		log.Println("Broadcasting for", v.vertexID)
		newState := v.state.shortestPathSize + 1
		outboxChannel <- Envelope{destinationVertexID: nbdID, message: Message{candidateShortestPath: newState}}
	}

}

func inboxWorker(vertexStore map[uint32]*Vertex, inboxChannel chan Envelope) {
	log.Println("Worker spawned")
	defer wg.Done()
	select {
	case envelope := <-inboxChannel:
		log.Println("Processing message for", envelope.destinationVertexID)
		v := vertexStore[envelope.destinationVertexID]
		v.compute(envelope.message)
		log.Println("State of", envelope.destinationVertexID, "now is", v.state)
	case <-time.After(1 * time.Second):
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
		if v.state.shortestPathSize != math.MaxUint32 {
			v.broadcast(outboxChannel)
		}
	}

	select {
	case envelope := <-outboxChannel:
		log.Println(envelope, "sent to inboxChannel")
		inboxChannel <- envelope
	case <-time.After(2 * time.Second):
		return
	}
}

func displayFinalResults(vertexStore map[uint32]*Vertex) {
	for _, v := range vertexStore {
		log.Println("ID:", v.vertexID, "STATE:", v.state)
	}
}

func ingestGraph(vertexStore map[uint32]*Vertex, path string) {

}

// TODO:
// Create graph_utils package with a graph loader (from edgeset etc)
// go routinize vertex execution
// Figure out the unexported names thing
// Fix broadcasting problem (only should be done for active nodes)
// Fix issue where node 3 only gets the guaranteed correct value after 5 steps instead of 3
var wg sync.WaitGroup

func main() {
	log.Println("Initializing")
	var vertexStore = make(map[uint32]*Vertex)
	var inboxChannel = make(chan Envelope, 100)
	var outboxChannel = make(chan Envelope, 100)

	log.Println("Loading vertices")
	vertexStore[1] = &Vertex{vertexID: 1, state: VertexState{shortestPathSize: math.MaxUint32}, outNeighbourIds: []uint32{2}}
	vertexStore[2] = &Vertex{vertexID: 2, state: VertexState{shortestPathSize: math.MaxUint32}, outNeighbourIds: []uint32{3, 4}}
	vertexStore[3] = &Vertex{vertexID: 3, state: VertexState{shortestPathSize: math.MaxUint32}, outNeighbourIds: []uint32{}}
	vertexStore[4] = &Vertex{vertexID: 4, state: VertexState{shortestPathSize: math.MaxUint32}, outNeighbourIds: []uint32{3, 5}}
	vertexStore[5] = &Vertex{vertexID: 5, state: VertexState{shortestPathSize: math.MaxUint32}, outNeighbourIds: []uint32{}}

	log.Println("Seed sent")
	inboxChannel <- Envelope{destinationVertexID: 1, message: Message{candidateShortestPath: 0}}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go inboxWorker(vertexStore, inboxChannel)
		wg.Wait()
		outboxWorker(vertexStore, outboxChannel, inboxChannel)
	}

	displayFinalResults(vertexStore)

}
