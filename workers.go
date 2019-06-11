package main

import (
	"log"
	"math"
)

func InboxWorker(vertexStore map[uint32]*Vertex, inboxChannel chan Envelope) {
	log.Println("Inbox worker spawned")
	defer wg.Done()

	for envelope := range inboxChannel {
		log.Println("Processing message for", envelope.destinationVertexID)
		v := vertexStore[envelope.destinationVertexID]
		v.Compute(envelope.message)
		log.Println("State of", envelope.destinationVertexID, "now is", v.state)
	}
}

func OutboxWorker(vertexStore map[uint32]*Vertex, outboxChannel chan Envelope, inboxChannel chan Envelope) {
	log.Println("Executing outboxWorker")
	for _, v := range vertexStore {
		if v.state.shortestPathSize != math.MaxUint32 && v.voteToHalt == false {
			v.Broadcast(outboxChannel)
		}
	}

	close(outboxChannel)

	for envelope := range outboxChannel {
		log.Println(envelope, "sent to inboxChannel")
		inboxChannel <- envelope
	}

	close(inboxChannel)
}
