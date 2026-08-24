package main

import (
	"log"
	"time"

	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/raft"
)

func main() {
	addresses := map[int]string{
		0: "localhost:5000",
		1: "localhost:5001",
		2: "localhost:5002",
		3: "localhost:5003",
		4: "localhost:5004",
	}

	nodes := make(map[int]*raft.RaftNode)

	for id := 0; id < 5; id++ {
		nodes[id] = raft.NewRaftNode()
	}

	for id, node := range nodes {
		go func(id int, node *raft.RaftNode) {
			if err := node.RunServer(addresses[id]); err != nil {
				log.Fatal(err)
			}
		}(id, node)
	}

	time.Sleep(time.Second)

	for id, node := range nodes {
		peers := make(map[int]string)

		for peerID, address := range addresses {
			if peerID != id {
				peers[peerID] = address
			}
		}

		node.RegisterClients(peers)
	}

	for _, node := range nodes {
		go node.DetectElectionTimeout()
		node.StartMessageApplier()
	}

	select {}
}
