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

func (node *RaftNode) HandleAppendEntries(request *raftproto.AppendEntriesRequest) *raftproto.AppendEntriesReply {
	if request.Term < node.currentTerm {
		return &raftproto.AppendEntriesReply{
			Term:    node.currentTerm,
			Success: false,
		}
	}

	if request.PrevLogTerm != node.currentTerm || request.PrevLogIndex != int64(len(node.logEntries)-1) {
		return &raftproto.AppendEntriesReply{
			Term:    node.currentTerm,
			Success: false,
		}
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

	return &raftproto.AppendEntriesReply{
		Term:    node.currentTerm,
		Success: true,
	}
}
