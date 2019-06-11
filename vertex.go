package main

import "log"

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

func (v *Vertex) Compute(message Message) {
	if v.state.shortestPathSize > message.candidateShortestPath {
		v.state.shortestPathSize = message.candidateShortestPath

		// If a vertex has updated its state, it will want to broadcast it
		v.voteToHalt = false
	}
}

func (v *Vertex) Broadcast(outboxChannel chan Envelope) {
	log.Println("Broadcasting for", v.vertexID)

	for _, nbdID := range v.outNeighbourIds {
		newState := v.state.shortestPathSize + 1
		outboxChannel <- Envelope{destinationVertexID: nbdID, message: Message{candidateShortestPath: newState}}

		// As soon as the vertex has broadcasted its new state, it will want to stop
		v.voteToHalt = true
	}

}
