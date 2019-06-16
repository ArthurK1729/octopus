package main

import (
	"context"
)

const (
	masterPort = ":50051"
)

type masterserver struct {
	vertexSlaveRegistry VertexSlaveRegistry
}

func (s *masterserver) GetVertexSlaveRegistry(ctx context.Context, empty *Empty) (*VertexSlaveRegistry, error) {
	return &s.vertexSlaveRegistry, nil
}
