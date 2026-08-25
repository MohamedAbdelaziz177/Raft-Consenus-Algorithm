package main

import (
	"log"
	"net/http"
	"time"

	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/api"
	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/raft"
	"github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/storage"
)

func main() {

	// raft nodes themselves
	raftAddresses := map[int]string{
		0: "localhost:5000",
		1: "localhost:5001",
		2: "localhost:5002",
		3: "localhost:5003",
		4: "localhost:5004",
	}

	// api handlers
	handlersAddresses := map[int]string{
		0: "localhost:6000",
		1: "localhost:6001",
		2: "localhost:6002",
		3: "localhost:6003",
		4: "localhost:6004",
	}

	nodes := make(map[int]*raft.RaftNode)
	stores := make(map[int]*storage.KVStore)

	for id := 0; id < 5; id++ {
		nodes[id] = raft.NewRaftNode()
		stores[id] = storage.NewKVStore()
	}

	for id, node := range nodes {
		go func(id int, node *raft.RaftNode) {
			if err := node.RunServer(raftAddresses[id]); err != nil {
				log.Fatalf("Raft node %d failed: %v", id, err)
			}
		}(id, node)
	}

	time.Sleep(time.Second)

	for id, node := range nodes {
		peers := make(map[int]string)

		for peerID, address := range raftAddresses {
			if peerID != id {
				peers[peerID] = address
			}
		}

		node.RegisterClients(peers)
	}

	for id, node := range nodes {
		node.StartMessageApplier()
		stores[id].ListenToApplyChannel(node.ApplyCh)

		go node.DetectElectionTimeout()
	}

	for id := range nodes {
		go func(id int) {
			handler := api.NewHandler(
				nodes[id],
				stores[id],
			)

			router := api.NewRouter(&handler)

			log.Printf(
				"HTTP API for node %d listening on %s",
				id,
				handlersAddresses[id],
			)

			if err := http.ListenAndServe(
				handlersAddresses[id],
				router,
			); err != nil {
				log.Fatalf(
					"HTTP server for node %d failed: %v",
					id,
					err,
				)
			}
		}(id)
	}

	select {}

}
