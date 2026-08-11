package raft

import (
	"context"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
)

func (node *RaftNode) RequestVote(ctx context.Context, request *raftproto.RequestVoteRequest) (*raftproto.RequestVoteReply, error) {
	if request.Term > node.currentTerm {
		node.currentTerm = request.Term
		node.votedFor = -1
		node.state = Follower
	}

	if request.Term < node.currentTerm {
		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  false,
		}, nil
	}

	lastTerm := 0
	if node.logEntries != nil && len(node.logEntries) > 0 {
		lastTerm = int(node.logEntries[len(node.logEntries)-1].Term)
	}

	validLog := request.LastLogTerm > int64(lastTerm) ||
		(request.LastLogTerm == int64(lastTerm) && request.LastLogIndex >=
			int64(len(node.logEntries)-1))

	if node.currentTerm == request.Term && validLog && (node.votedFor == -1 || node.votedFor == request.CandidateId) {

		node.votedFor = request.CandidateId
		node.ResetElectionTimer()

		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  true,
		}, nil
	}

	return &raftproto.RequestVoteReply{
		FollowerTerm: node.currentTerm,
		VoteGranted:  false,
	}, nil
}

func (node *RaftNode) AppendEntries(ctx context.Context, request *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesReply, error) {

	if request.Term < node.currentTerm {
		return &raftproto.AppendEntriesReply{
			Term:    node.currentTerm,
			Success: false,
		}, nil
	}

	if request.Term > node.currentTerm {
		node.currentTerm = request.Term
		node.votedFor = -1
		node.state = Follower
	}

	lastLogIdx := len(node.logEntries) - 1

	if request.PrevLogIndex > -1 && request.PrevLogIndex > int64(lastLogIdx) {
		return &raftproto.AppendEntriesReply{
			Success: false,
			Term:    node.currentTerm,
		}, nil
	}

	prevLogItemTermAtReciver := node.logEntries[request.PrevLogIndex].Term
	if request.PrevLogIndex > -1 && prevLogItemTermAtReciver != request.PrevLogTerm {
		return &raftproto.AppendEntriesReply{
			Success: false,
			Term:    node.currentTerm,
		}, nil
	}

	for i, entry := range request.LogEntries {
		currIdx := int(request.PrevLogIndex) + 1 + i

		if currIdx < len(node.logEntries) {
			if node.logEntries[currIdx].Term != entry.Term {
				node.logEntries = node.logEntries[:currIdx]
				break
			}
		}
	}

	node.logEntries = append(node.logEntries, request.LogEntries...)

	if request.LeaderCommitIdx > node.commitIndex {
		node.commitIndex = min(request.LeaderCommitIdx, int64(len(node.logEntries)-1))
	}

	node.ResetElectionTimer()

	return &raftproto.AppendEntriesReply{
		Term:    node.currentTerm,
		Success: true,
	}, nil
}
