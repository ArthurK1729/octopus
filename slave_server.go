package main

import (
	"context"
	"log"
	"time"
)

const (
	slavePort = ":50052"
)

type slaveserver struct {
	vertexStore   map[uint32]*Vertex
	inboxChannel  chan Envelope
	outboxChannel chan Envelope
}

func (s *slaveserver) GetHeartbeat(ctx context.Context, empty *Empty) (*Heartbeat, error) {
	now := time.Now().Unix()
	return &Heartbeat{Timestamp: now}, nil
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

func (s *slaveserver) InitiateExecution(ctx context.Context, empty *Empty) (*Empty, error) {
	log.Println("Sending seed")

	for i := 0; i < 20; i++ {
		s.outboxChannel = make(chan Envelope, 100000)

		if i == 0 {
			s.inboxChannel = make(chan Envelope, 100000)
			s.inboxChannel <- Envelope{DestinationVertexID: 0, Message: &Message{CandidateShortestPath: 0}}
			close(s.inboxChannel)
		}

		wg.Add(1)
		go InboxWorker(s.vertexStore, s.inboxChannel)
		wg.Add(1)
		go InboxWorker(s.vertexStore, s.inboxChannel)
		wg.Add(1)
		go InboxWorker(s.vertexStore, s.inboxChannel)
		wg.Add(1)
		go InboxWorker(s.vertexStore, s.inboxChannel)
		wg.Add(1)
		go InboxWorker(s.vertexStore, s.inboxChannel)
		wg.Add(1)
		go InboxWorker(s.vertexStore, s.inboxChannel)
		wg.Wait()

		s.inboxChannel = make(chan Envelope, 100000)
		OutboxWorker(s.vertexStore, s.outboxChannel, s.inboxChannel)
		log.Println("ROUND OVER")
	}

	DisplayFinalResults(s.vertexStore)
	CountUnvisited(s.vertexStore)

	return &Empty{}, nil
}
