package raft

import (
	"context"
	"log"
	"sync"
	"time"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
)

const NO_OF_NODES int = 5

func (node *RaftNode) startElection() {

	node.mu.Lock()

	node.votedFor = node.id
	node.leaderID = -1
	node.State = Candidate
	node.currentTerm++
	electionTerm := node.currentTerm

	node.resetElectionTimer()

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

	log.Printf(
		"[Node %d] Starting election: term=%d lastLog=(idx=%d,term=%d)",
		node.id,
		electionTerm,
		lastLogIndex,
		lastLogTerm,
	)

	node.mu.Unlock()

	wg := sync.WaitGroup{}
	grantedVotes := 1

	for peerID, peerClient := range node.peers {

		wg.Add(1)

		go func(peerID int, peer raftproto.RaftServicesClient) {

			defer wg.Done()

			reqVoteRes, err := peer.RequestVote(
				context.Background(),
				requestVoteObj,
			)

			if err != nil {
				log.Printf(
					"[Node %d] RequestVote to Node %d failed: %v",
					node.id,
					peerID,
					err,
				)
				return
			}

			node.mu.Lock()
			defer node.mu.Unlock()

			log.Printf(
				"[Node %d] Received RequestVote reply from Node %d: "+
					"term=%d voteGranted=%t",
				node.id,
				peerID,
				reqVoteRes.FollowerTerm,
				reqVoteRes.VoteGranted,
			)

			if reqVoteRes.FollowerTerm > node.currentTerm {

				log.Printf(
					"[Node %d] Node %d has higher term: %d -> %d. Stepping down",
					node.id,
					peerID,
					node.currentTerm,
					reqVoteRes.FollowerTerm,
				)

				node.stepDownToFollower(reqVoteRes)
				return
			}

			if node.currentTerm != electionTerm {

				log.Printf(
					"[Node %d] Ignoring vote from Node %d: election term %d is no longer current (currentTerm=%d)",
					node.id,
					peerID,
					electionTerm,
					node.currentTerm,
				)

				return
			}

			if node.State != Candidate {
				log.Printf(
					"[Node %d] Ignoring vote from Node %d: node is no longer a candidate",
					node.id,
					peerID,
				)

				return
			}

			if reqVoteRes.VoteGranted {
				grantedVotes++

				log.Printf(
					"[Node %d] Vote granted by Node %d: votes=%d/%d",
					node.id,
					peerID,
					grantedVotes,
					NO_OF_NODES,
				)
			} else {
				log.Printf(
					"[Node %d] Vote rejected by Node %d: votes=%d/%d",
					node.id,
					peerID,
					grantedVotes,
					NO_OF_NODES,
				)
			}

		}(peerID, peerClient)
	}

	wg.Wait()

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.currentTerm != electionTerm {
		log.Printf(
			"[Node %d] Election for term %d finished but current term is %d. Not becoming leader",
			node.id,
			electionTerm,
			node.currentTerm,
		)
		return
	}

	if node.State != Candidate {
		log.Printf(
			"[Node %d] Election for term %d finished but node is no longer a candidate",
			node.id,
			electionTerm,
		)
		return
	}

	log.Printf(
		"[Node %d] Election finished: votes=%d/%d term=%d",
		node.id,
		grantedVotes,
		NO_OF_NODES,
		electionTerm,
	)

	node.levelUpToLeader(grantedVotes)
}

func (node *RaftNode) stepDownToFollower(reqVoteRes *raftproto.RequestVoteReply) {

	log.Printf(
		"[Node %d] Stepping down to FOLLOWER: term %d -> %d",
		node.id,
		node.currentTerm,
		reqVoteRes.FollowerTerm,
	)

	node.State = Follower
	node.leaderID = -1
	node.currentTerm = reqVoteRes.FollowerTerm
	node.votedFor = -1
	node.resetElectionTimer()

	if node.heartbeatTicker != nil {
		node.heartbeatTicker.Stop()
		node.heartbeatTicker = nil
	}
}

func (node *RaftNode) levelUpToLeader(grantedVotes int) {

	if node.State != Candidate {
		return
	}

	if 2*grantedVotes <= NO_OF_NODES {

		log.Printf(
			"[Node %d] Failed to win election: votes=%d/%d term=%d",
			node.id,
			grantedVotes,
			NO_OF_NODES,
			node.currentTerm,
		)

		return
	}

	node.State = Leader
	node.leaderID = node.id

	node.nextIndex = make(map[int]int64)
	node.matchIndex = make(map[int]int64)

	for id := range node.peers {
		node.nextIndex[id] = int64(len(node.logEntries))
		node.matchIndex[id] = int64(-1)
	}

	log.Printf(
		"[Node %d] Became LEADER: term=%d votes=%d/%d logLength=%d",
		node.id,
		node.currentTerm,
		grantedVotes,
		NO_OF_NODES,
		len(node.logEntries),
	)

	go node.sendHeartBeatToAllPeers()

	node.heartbeatTicker = time.NewTicker(150 * time.Millisecond)

	go node.listenToHeartBeatTicker(context.Background())
}
