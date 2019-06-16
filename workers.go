package main

import (
	"log"
	"math"
)

func InboxWorker(vertexStore map[uint32]*Vertex, inboxChannel chan Envelope) {
	log.Println("Inbox worker spawned")
	defer wg.Done()

	for envelope := range inboxChannel {
		log.Println("Processing message for", envelope.DestinationVertexID)
		v := vertexStore[envelope.DestinationVertexID]
		v.Compute(*envelope.Message)
		log.Println("State of", envelope.DestinationVertexID, "now is", v.State)
	}
}

func OutboxWorker(vertexStore map[uint32]*Vertex, outboxChannel chan Envelope, inboxChannel chan Envelope) {
	log.Println("Executing outboxWorker")
	for _, v := range vertexStore {
		if v.State.ShortestPathSize != math.MaxUint32 && v.VoteToHalt == false {
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
