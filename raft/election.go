package raft

import (
	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
)

func (node *RaftNode) HandleRequestVote(request *raftproto.RequestVoteRequest) *raftproto.RequestVoteReply {
	if node.currentTerm >= request.Term {
		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  false,
		}
	}

	if request.LastLogTerm < node.currentTerm || request.LastLogIndex < int64(len(node.logEntries)-1) {
		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  false,
		}
	}

	return &raftproto.RequestVoteReply{
		FollowerTerm: node.currentTerm,
		VoteGranted:  true,
	}
}
