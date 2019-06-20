package main

import (
	"context"
	"log"
	"math"
	"sync"
	"time"
)

const (
	slavePort1 = ":50052"
	slavePort2 = ":50053"
)

type slaveserver struct {
	slaveIdentifier  *SlaveIdentifier
	masterClient     MasterClient
	vertexStore      map[uint32]*Vertex
	inboxChannel     chan Envelope
	outboxChannel    chan Envelope
	concurrencyLevel uint8
	wg               sync.WaitGroup
	ctx              context.Context
}

func (s *slaveserver) GetHeartbeat(ctx context.Context, empty *Empty) (*Heartbeat, error) {
	now := time.Now().Unix()
	return &Heartbeat{Timestamp: now}, nil
}

func (s *slaveserver) PopulateInbox(ctx context.Context, envelopes *Envelopes) (*Empty, error) {
	for _, envelope := range envelopes.Envelopes {
		s.inboxChannel <- *envelope
	}

	return &Empty{}, nil
}

func (s *slaveserver) LoadGraphPartition(ctx context.Context, vertices *Vertices) (*Empty, error) {
	log.Println("Initializing vertexStore")
	s.vertexStore = make(map[uint32]*Vertex)

	for _, vertex := range vertices.Vertices {
		s.vertexStore[vertex.VertexID] = vertex
	}
	log.Println("vertexStore initialized")
	return &Empty{}, nil
}

// func (s *slaveserver) InitiateExecution(ctx context.Context, empty *Empty) (*Empty, error) {
// 	log.Println("Sending seed")

// 	for i := 0; i < 20; i++ {
// 		s.outboxChannel = make(chan Envelope, 100000)

// 		if i == 0 {
// 			s.inboxChannel = make(chan Envelope, 100000)
// 			s.inboxChannel <- Envelope{DestinationVertexID: 0, Message: &Message{CandidateShortestPath: 0}}
// 			close(s.inboxChannel)
// 		}

// 		wg.Add(1)
// 		go InboxWorker(s.vertexStore, s.inboxChannel)
// 		wg.Add(1)
// 		go InboxWorker(s.vertexStore, s.inboxChannel)
// 		wg.Add(1)
// 		go InboxWorker(s.vertexStore, s.inboxChannel)
// 		wg.Add(1)
// 		go InboxWorker(s.vertexStore, s.inboxChannel)
// 		wg.Add(1)
// 		go InboxWorker(s.vertexStore, s.inboxChannel)
// 		wg.Add(1)
// 		go InboxWorker(s.vertexStore, s.inboxChannel)
// 		wg.Wait()

// 		s.inboxChannel = make(chan Envelope, 100000)
// 		OutboxWorker(s.vertexStore, s.outboxChannel, s.inboxChannel)
// 		log.Println("ROUND OVER")
// 	}

// 	DisplayFinalResults(s.vertexStore)
// 	CountUnvisited(s.vertexStore)

// 	return &Empty{}, nil
// }

func (s *slaveserver) InitiateExecution(ctx context.Context, empty *Empty) (*Empty, error) {
	s.outboxChannel = make(chan Envelope, 100000)
	close(s.inboxChannel)
	log.Println("Spinning up InboxWorkers")
	for i := 0; i < int(s.concurrencyLevel); i++ {
		s.wg.Add(1)
		go s.InboxWorker()
	}

	s.wg.Wait()
	log.Println("InboxWorkers done")
	s.inboxChannel = make(chan Envelope, 100000)

	DisplayFinalResults(s.vertexStore)
	CountUnvisited(s.vertexStore)

	// Should really return SlaveDone as a response instead of a separate call
	log.Println("Message processing done. Notifying master")
	s.masterClient.SlaveDone(s.ctx, &SlaveIdentifier{SlaveIdentifier: s.slaveIdentifier.SlaveIdentifier})
	return &Empty{}, nil
}

func (s *slaveserver) InitiateBroadcast(ctx context.Context, empty *Empty) (*Empty, error) {

	// for i := 0; i < int(s.concurrencyLevel); i++ {
	// 	s.wg.Add(1)
	// 	go s.OutboxWorker()
	// }

	// s.wg.Wait()

	// Make OutboxWorker concurrent
	log.Println("Executing BroadcastWorker")
	s.BroadcastWorker()
	close(s.outboxChannel)
	// FINISH BROADCASTING LOGIC
	log.Println("Broadcasting...")

	log.Println("Broadcasting done. Notifying master")
	s.masterClient.SlaveDone(s.ctx, &SlaveIdentifier{SlaveIdentifier: s.slaveIdentifier.SlaveIdentifier})

	return &Empty{}, nil
}

func (s *slaveserver) InboxWorker() {
	log.Println("Inbox worker spawned")
	defer s.wg.Done()

	for envelope := range s.inboxChannel {
		log.Println("Processing message for", envelope.DestinationVertexID)
		v := s.vertexStore[envelope.DestinationVertexID]
		v.Compute(*envelope.Message)
		log.Println("State of", envelope.DestinationVertexID, "now is", v.State)
	}
}

func (s *slaveserver) BroadcastWorker() {
	log.Println("Executing BroadcastWorker")
	for _, v := range s.vertexStore {
		if v.State.ShortestPathSize != math.MaxUint32 && v.VoteToHalt == false {
			v.Broadcast(s.outboxChannel)
		}
	}
}

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
