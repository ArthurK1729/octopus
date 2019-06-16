package main

import (
	"bufio"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

// Reads in an adjlist and returns vertexStore
func ReadAdjlist(path string) (map[uint32]*Vertex, error) {
	log.Println("Reading", path)
	log.Println("Initializing vertex store")

	vertexStore := make(map[uint32]*Vertex)

	file, err := os.Open(path)
	defer file.Close()

	if err != nil {
		return map[uint32]*Vertex{}, err
	}

	reader := bufio.NewReader(file)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		nodes := strings.Split(line, " ")
		log.Println("Loading node tuple: ", nodes)
		originNode, _ := strconv.Atoi(strings.TrimSpace(nodes[0]))
		targetNodes := make([]uint32, len(nodes[1:]))

		for index, node := range nodes[1:] {
			n, _ := strconv.Atoi(strings.TrimSpace(node))
			targetNodes[index] = uint32(n)
		}

		vertexStore[uint32(originNode)] = &Vertex{
			VertexID:        uint32(originNode),
			State:           &VertexState{ShortestPathSize: math.MaxUint32},
			OutNeighbourIds: targetNodes,
			VoteToHalt:      false,
		}

	}

	if err != io.EOF {
		log.Printf("Failed to read graph: %v\n", err)
	}

	return vertexStore, nil
}

// func WriteResult(map[uint32]*Vertex) error {

// }
