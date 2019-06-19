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
	vertexSlaveRegistry  VertexSlaveRegistry
	slaveCount           uint32
	slaveConnectionStore map[uint32]SlaveClientInfo
	mut                  *sync.Mutex
}

type SlaveClientInfo struct {
	slaveHost             string
	slaveClientConnection SlaveClient
}

func (s *masterserver) GetVertexSlaveRegistry(ctx context.Context, empty *Empty) (*VertexSlaveRegistry, error) {
	return &s.vertexSlaveRegistry, nil
}

func (s *masterserver) incrementSlaveCount() {
	s.mut.Lock()
	s.slaveCount++
	s.mut.Unlock()
}

func (s *masterserver) RegisterSlave(ctx context.Context, slaveHost *SlaveHost) (*SlaveIdentifier, error) {
	log.Println("Registering slave", slaveHost.SlaveHost)
	// Change localhost to proper hostname later
	s.incrementSlaveCount()
	s.vertexSlaveRegistry.Registry[s.slaveCount] = "localhost:" + slaveHost.SlaveHost

	return &SlaveIdentifier{SlaveIdentifier: s.slaveCount}, nil
}
