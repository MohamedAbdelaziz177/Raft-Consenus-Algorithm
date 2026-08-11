package raft

import (
	"context"
	"sync"
	"time"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
)

const NO_OF_NODES int = 5

func (node *RaftNode) startElection() {

	node.votedFor = node.id
	node.state = Candidate
	node.currentTerm++

	var lastLogIndex int64 = -1
	var lastLogTerm int64 = 0

	if len(node.logEntries) > 0 {
		lastLogIndex = int64(len(node.logEntries) - 1)
		lastLogTerm = node.logEntries[lastLogIndex].Term
	}

	requestVoteObj := &raftproto.RequestVoteRequest{
		Term:         node.currentTerm,
		CandidateId:  node.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	wg := sync.WaitGroup{}
	grantedVotes := 1

	for _, PeerClient := range node.peers {

		wg.Add(1)
		go func(peer raftproto.RaftServicesClient) {

			defer wg.Done()

			reqVoteRes, err := peer.RequestVote(context.Background(), requestVoteObj)
			if err != nil {
				return
			}

			node.mu.Lock()
			defer node.mu.Unlock()

			if reqVoteRes.FollowerTerm > node.currentTerm {
				node.stepDownToFollower(reqVoteRes)
			}

			if reqVoteRes.VoteGranted == true {
				grantedVotes++
			}

		}(PeerClient)
	}

	wg.Wait()

	node.mu.Lock()
	node.levelUpToLeader(grantedVotes)
	node.mu.Unlock()

}

func (node *RaftNode) stepDownToFollower(reqVoteRes *raftproto.RequestVoteReply) {
	node.state = Follower
	node.currentTerm = reqVoteRes.FollowerTerm
	node.votedFor = -1
	node.ResetElectionTimer()
}

func (node *RaftNode) levelUpToLeader(grantedVotes int) {
	if node.state == Candidate && 2*grantedVotes >= NO_OF_NODES {
		node.state = Leader

		node.nextIndex = make(map[int]int64)
		node.matchIndex = make(map[int]int64)

		for id := range node.peers {
			node.nextIndex[id] = int64(len(node.logEntries))
			node.matchIndex[id] = int64(-1)
		}

		node.heartbeatTicker = time.NewTicker(150 * time.Millisecond)
		// notes: send heart beat directly & then run the ticker
		// go node.StartHeartBeatLoop()
	}
}
