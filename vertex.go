package main

import "log"

// // Envelope contains the destination vertex id and the message it needs to process
// type Envelope struct {
// 	message             Message
// 	destinationVertexID uint32
// }

// // Message contains the data that must be supplied into Compute
// type Message struct {
// 	candidateShortestPath uint32
// }

// // VertexState is the current state of a vertex
// type VertexState struct {
// 	shortestPathSize uint32
// }

// type Vertex struct {
// 	vertexID        uint32
// 	state           VertexState
// 	outNeighbourIds []uint32
// 	voteToHalt      bool
// }

func (v *Vertex) Compute(message Message) {
	if v.State.ShortestPathSize > message.CandidateShortestPath {
		v.State.ShortestPathSize = message.CandidateShortestPath

		// If a vertex has updated its state, it will want to broadcast it
		v.VoteToHalt = false
	}
}

func (v *Vertex) Broadcast(outboxChannel chan Envelope) {
	log.Println("Broadcasting for", v.VertexID)

	for _, nbdID := range v.OutNeighbourIds {
		newState := v.State.ShortestPathSize + 1
		outboxChannel <- Envelope{DestinationVertexID: nbdID, Message: &Message{CandidateShortestPath: newState}}

		// As soon as the vertex has broadcasted its new state, it will want to stop
		v.VoteToHalt = true
	}

}
