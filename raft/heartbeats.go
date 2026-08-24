package raft

import (
	"context"
	"log"
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

func (node *RaftNode) listenToHeartBeatTicker(ctx context.Context) {
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

			node.sendHeartBeatToAllPeers()

		case <-ctx.Done():
			return
		}
	}
}

func (node *RaftNode) sendHeartBeatToAllPeers() {
	for id, peerClient := range node.peers {
		node.mu.Lock()

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

		aeRequest := &raftproto.AppendEntriesRequest{
			Term:            node.currentTerm,
			LeaderId:        node.id,
			LeaderCommitIdx: node.commitIndex,
			LogEntries:      node.logEntries[nextIdx:],
			PrevLogIndex:    aeCtx.prevLogIdx,
			PrevLogTerm:     aeCtx.prevLogTerm,
		}

		log.Printf(
			"[Node %d] Sending AppendEntries to Node %d: term=%d prevLog=(idx=%d,term=%d) entries=%d commitIndex=%d nextIndex=%d",
			node.id,
			id,
			node.currentTerm,
			prevLogIdx,
			prevLogTerm,
			len(aeRequest.LogEntries),
			node.commitIndex,
			nextIdx,
		)

		node.mu.Unlock()

		go func(aeCtx appendEntriesHandlerContext) {
			reply, err := peerClient.AppendEntries(
				context.Background(),
				aeRequest,
			)

			if err != nil {
				log.Printf(
					"[Node %d] AppendEntries to Node %d failed: %v",
					node.id,
					aeCtx.id,
					err,
				)
				return
			}

			node.handleAppendEntriesReply(
				aeCtx.id,
				aeRequest,
				reply,
			)
		}(aeCtx)
	}
}

func (node *RaftNode) DetectElectionTimeout() {
	for {
		<-node.electionTimer.C

		node.mu.Lock()
		state := node.state
		node.mu.Unlock()

		if state != Leader {
			log.Printf(
				"[Node %d] Election timeout detected",
				node.id,
			)

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

func (node *RaftNode) handleAppendEntriesReply(
	peerId int,
	aeRequest *raftproto.AppendEntriesRequest,
	reply *raftproto.AppendEntriesReply,
) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if reply.Term > node.currentTerm {
		log.Printf(
			"[Node %d] Received higher term from Node %d: %d -> %d. Stepping down",
			node.id,
			peerId,
			node.currentTerm,
			reply.Term,
		)

		node.currentTerm = reply.Term
		node.state = Follower
		node.votedFor = -1
		node.resetElectionTimer()
		return
	}

	if node.state != Leader {
		return
	}

	if !reply.Success {
		oldNextIndex := node.nextIndex[peerId]

		if node.nextIndex[peerId] > 0 {
			node.nextIndex[peerId]--
		}

		log.Printf(
			"[Node %d] AppendEntries rejected by Node %d: nextIndex %d -> %d",
			node.id,
			peerId,
			oldNextIndex,
			node.nextIndex[peerId],
		)

		return
	}

	matchIdx := aeRequest.PrevLogIndex + int64(len(aeRequest.LogEntries))

	oldMatchIndex := node.matchIndex[peerId]
	oldNextIndex := node.nextIndex[peerId]

	node.matchIndex[peerId] = matchIdx
	node.nextIndex[peerId] = matchIdx + 1

	log.Printf(
		"[Node %d] AppendEntries accepted by Node %d: matchIndex %d -> %d, nextIndex %d -> %d",
		node.id,
		peerId,
		oldMatchIndex,
		node.matchIndex[peerId],
		oldNextIndex,
		node.nextIndex[peerId],
	)

	oldCommitIndex := node.commitIndex

	node.incrLeaderCommitIdx()

	if node.commitIndex != oldCommitIndex {
		log.Printf(
			"[Node %d] commitIndex advanced: %d -> %d",
			node.id,
			oldCommitIndex,
			node.commitIndex,
		)
	}
}

func (node *RaftNode) incrLeaderCommitIdx() {
	for i := node.commitIndex + 1; i < int64(len(node.logEntries)); i++ {
		count := 1

		for _, matchIdx := range node.matchIndex {
			if matchIdx >= i {
				count++
			}
		}

		if count*2 > NO_OF_NODES &&
			node.logEntries[i].Term == node.currentTerm {

			node.commitIndex = i
		}
	}
}
