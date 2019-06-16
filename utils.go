package main

import (
	"log"
	"math"
)

func DisplayFinalResults(vertexStore map[uint32]*Vertex) {
	for _, v := range vertexStore {
		log.Println("ID:", v.VertexID, "STATE:", v.State)
	}
}

func CountUnvisited(vertexStore map[uint32]*Vertex) {
	count := 0
	for _, v := range vertexStore {
		if v.State.ShortestPathSize == math.MaxUint32 {
			count++
		}
	}
	log.Println("Number of unvisited:", count)
}
