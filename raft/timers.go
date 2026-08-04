package raft

import (
	"math/rand"
	"time"
)

func (rn *RaftNode) DetectElectionTimeout() {
	for {
		<-rn.electionTimer.C
		rn.mu.Lock()
		if rn.state != Leader {
			//rn.startElection()
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) ResetElectionTimer() {
	timeout := time.Duration(rand.Intn(400)+150) * time.Millisecond
	if !rn.electionTimer.Stop() {
		select {
		case <-rn.electionTimer.C:
		default:
		}
	}

	rn.electionTimer.Reset(timeout)
}
