package main

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
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
	log.Println("Populating inbox with foreign messages")
	for _, envelope := range envelopes.Envelopes {
		s.inboxChannel <- *envelope
	}
	log.Println("Done populating inbox with foreign messages")

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

func (s *slaveserver) InitiateExecution(ctx context.Context, empty *Empty) (*Empty, error) {
	// No more incoming messages accepted
	close(s.inboxChannel)

	log.Println("Spinning up InboxWorkers")
	for i := 0; i < int(s.concurrencyLevel); i++ {
		log.Println("Worker", i, "initialized")
		s.wg.Add(1)
		go s.InboxWorker()
	}

	s.wg.Wait()

	log.Println("InboxWorkers done")
	s.inboxChannel = make(chan Envelope, 100000)
	s.outboxChannel = make(chan Envelope, 100000)

	DisplayFinalResults(s.vertexStore)
	CountUnvisited(s.vertexStore)

	// Should really return SlaveDone as a response instead of a separate call
	log.Println("Message processing done. Notifying master")
	s.masterClient.SlaveDone(s.ctx, &SlaveIdentifier{SlaveIdentifier: s.slaveIdentifier.SlaveIdentifier})
	return &Empty{}, nil
}

func (s *slaveserver) populateOwnInbox(envelopes []*Envelope) {
	log.Println("Populating inbox with home messages")
	for _, envelope := range envelopes {
		s.inboxChannel <- *envelope
	}
	log.Println("Done populating inbox with home messages")
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

	log.Println("Broadcasting...")

	log.Println("Getting slave registry from master")
	slaveRegistry, err := s.masterClient.GetVertexSlaveRegistry(s.ctx, &Empty{})
	if err != nil {
		log.Fatalln("Could not obtain slave registry from master:", err)
	}
	log.Println("Slave registry is", slaveRegistry.Registry)

	vertexShipments := make(map[uint32][]*Envelope)

	for i := 1; i <= len(slaveRegistry.Registry); i++ {
		vertexShipments[uint32(i)] = make([]*Envelope, 0)
	}

	log.Println("Shipment slices initialized")
	// Fucking bug down here. Arrays keep getting duplicate IDs
	for envelope := range s.outboxChannel {
		destinationSlaveID := (envelope.DestinationVertexID % uint32(len(slaveRegistry.Registry))) + 1
		// Fix this later. If I don't create a new variable for envelope, the range will just keep
		// mutating the envelope variable but keeping the pointer the same, leading to duplication
		var env = envelope
		vertexShipments[destinationSlaveID] = append(vertexShipments[destinationSlaveID], &env)
	}

	log.Println("Outbound messages sorted by slave ID")
	log.Println("Vertex shipment:", vertexShipments)
	for slaveID, envelopeShipment := range vertexShipments {
		// If destination is inbound, just funnel the messages into own inbox
		if slaveID == s.slaveIdentifier.SlaveIdentifier {
			log.Println("Sending to myself:", envelopeShipment)
			s.populateOwnInbox(envelopeShipment)
			continue
		}
		// Otherwise, RPC the vertices to respective slaves
		if len(envelopeShipment) > 0 {
			slaveHost := slaveRegistry.Registry[slaveID]
			conn, err := grpc.Dial(slaveHost, grpc.WithInsecure())
			if err != nil {
				log.Fatalln("Cannot connect to slave", slaveHost)
			}
			slaveClient := NewSlaveClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			slaveClient.PopulateInbox(ctx, &Envelopes{Envelopes: envelopeShipment})
			log.Println("Sent envelope shipment to", slaveHost)
		}
	}

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
