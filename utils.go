package main

import (
	"log"
	"math"
)

func DisplayFinalResults(vertexStore map[uint32]*Vertex) {
	for _, v := range vertexStore {
		log.Println("ID:", v.vertexID, "STATE:", v.state)
	}
}

func CountUnvisited(vertexStore map[uint32]*Vertex) {
	count := 0
	for _, v := range vertexStore {
		if v.state.shortestPathSize == math.MaxUint32 {
			count++
		}
	}
	log.Println("Number of unvisited:", count)
}
