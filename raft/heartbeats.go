package raft

import (
	"context"
	"math/rand"
	"time"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
)

type appendEntriesHandlerContext struct {
	id          int
	client      raftproto.RaftServicesClient
	prevLogIdx  int64
	prevLogTerm int64
}

func (node *RaftNode) sendHeartBeatToAllPeers() {

	for id, peerClient := range node.peers {

		nextIdx := node.nextIndex[id]
		prevLogIdx := nextIdx - 1
		prevLogTerm := int64(-1)

		if prevLogIdx >= 0 {
			prevLogTerm = node.logEntries[prevLogIdx].Term
		}

		aeCtx := appendEntriesHandlerContext{
			prevLogIdx:  prevLogIdx,
			prevLogTerm: prevLogTerm,
			id:          id,
			client:      peerClient,
		}

		go func(aeCtx appendEntriesHandlerContext) {
			_, err := peerClient.AppendEntries(context.Background(), &raftproto.AppendEntriesRequest{
				Term:            node.currentTerm,
				LeaderId:        node.id,
				LeaderCommitIdx: node.commitIndex,
				LogEntries:      node.logEntries[nextIdx:],
				PrevLogIndex:    aeCtx.prevLogIdx,
				PrevLogTerm:     aeCtx.prevLogTerm,
			})

			if err != nil {
				return
			}

		}(aeCtx)
	}
}

func (node *RaftNode) listenToHeartBeatTicker() {
	for {
		select {
		case <-node.heartbeatTicker.C:
			node.mu.Lock()
			state := node.state
			node.mu.Unlock()

			if state != Leader {
				node.heartbeatTicker.Stop()
				return
			}

			go node.sendHeartBeatToAllPeers()
		}
	}
}

func (node *RaftNode) detectElectionTimeout() {
	for {
		<-node.electionTimer.C
		node.mu.Lock()
		state := node.state
		node.mu.Unlock()
		if state != Leader {
			node.startElection()
		}

	}
}

func (node *RaftNode) resetElectionTimer() {
	timeout := time.Duration(rand.Intn(400)+450) * time.Millisecond
	if !node.electionTimer.Stop() {
		select {
		case <-node.electionTimer.C:
		default:
		}
	}

	node.electionTimer.Reset(timeout)
}

func (node *RaftNode) handleAppendEntriesReply(reply *raftproto.AppendEntriesReply) {
	if reply.Term > node.currentTerm {
		node.currentTerm++
		node.state = Follower
		node.votedFor = -1
	}
}
