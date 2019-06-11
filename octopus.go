package main

import (
	"flag"
	"log"
	"sync"
)

func slaveProcess() {
	log.Println("Running in slave mode")

	log.Println("Initializing slave")
	var vertexStore map[uint32]*Vertex
	var inboxChannel chan Envelope
	var outboxChannel chan Envelope

	log.Println("Ingesting vertices")
	vertexStore, err := ReadAdjlist("data/facebook_social_graph.adjlist")

	if err != nil {
		log.Println("Couldn't read in graph. Exiting")
		panic("Couldn't read in graph")
	}

	log.Println("Sending seed")

	for i := 0; i < 20; i++ {
		outboxChannel = make(chan Envelope, 100000)

		if i == 0 {
			inboxChannel = make(chan Envelope, 100000)
			inboxChannel <- Envelope{destinationVertexID: 0, message: Message{candidateShortestPath: 0}}
			close(inboxChannel)
		}

		wg.Add(1)
		go InboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go InboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go InboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go InboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go InboxWorker(vertexStore, inboxChannel)
		wg.Add(1)
		go InboxWorker(vertexStore, inboxChannel)
		wg.Wait()

		inboxChannel = make(chan Envelope, 100000)
		OutboxWorker(vertexStore, outboxChannel, inboxChannel)
		log.Println("ROUND OVER")
	}

	DisplayFinalResults(vertexStore)
	CountUnvisited(vertexStore)
}

var wg sync.WaitGroup

// TODO:
// Intelligent partitioning scheme. Don't load the whole graph into memory, rather
// read lines one by one and ship them to other nodes once buffer limit is reached.
// voteToHalt might not be threadsafe
// Verify results with single source shortest path in NetworkX
// What do I do with nodes from other connected components that will never be touched? Their voteToHalt will remain false.
// https://www.youtube.com/watch?v=YEKjSzIwAdA try select default too?
// Implement multi-algorithm message passing
func main() {
	modePtr := flag.String("mode", "master", "master or slave run mode")

	log.Println(*modePtr)
	flag.Parse()
	if *modePtr == "slave" {
		slaveProcess()
	} else if *modePtr == "master" {
		// run masterProcess
	}

}
