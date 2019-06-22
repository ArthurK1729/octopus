package main

import (
	"context"
	"log"
	"sync"
)

const (
	masterPort = ":50051"
)

type masterserver struct {
	vertexSlaveRegistry VertexSlaveRegistry
	slaveCount          uint32
	// Replace below with *SlaveClientInfo
	slaveConnectionStore map[uint32]SlaveClientInfo
	mut                  *sync.Mutex
}

type SlaveClientInfo struct {
	slaveHost             string
	slaveClientConnection SlaveClient
	done                  *bool
}

func (s *masterserver) GetVertexSlaveRegistry(ctx context.Context, empty *Empty) (*VertexSlaveRegistry, error) {
	return &s.vertexSlaveRegistry, nil
}

func (s *masterserver) incrementSlaveCount() {
	s.mut.Lock()
	s.slaveCount++
	s.mut.Unlock()
}

func (s *masterserver) markSlaveAsDone(slaveID uint32) {
	s.mut.Lock()
	slaveClient := s.slaveConnectionStore[slaveID]
	*slaveClient.done = true
	s.mut.Unlock()
}

func (s *masterserver) markSlavesAsNotDone() {
	s.mut.Lock()

	for slaveID := range s.slaveConnectionStore {
		slaveClient := s.slaveConnectionStore[slaveID]
		*slaveClient.done = false
	}

	s.mut.Unlock()
}

func (s *masterserver) RegisterSlave(ctx context.Context, slaveHost *SlaveHost) (*SlaveIdentifier, error) {
	log.Println("Registering slave", slaveHost.SlaveHost)
	// Change localhost to proper hostname later
	s.incrementSlaveCount()
	s.vertexSlaveRegistry.Registry[s.slaveCount] = "octopus-service:" + slaveHost.SlaveHost

	return &SlaveIdentifier{SlaveIdentifier: s.slaveCount}, nil
}

func (s *masterserver) SlaveDone(ctx context.Context, slaveIdentifier *SlaveIdentifier) (*Empty, error) {
	log.Println("Marking", slaveIdentifier.SlaveIdentifier, "as done.")
	s.markSlaveAsDone(slaveIdentifier.SlaveIdentifier)

	return &Empty{}, nil
}
