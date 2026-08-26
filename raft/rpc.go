package raft

import (
	"context"
	"log"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
)

func (node *RaftNode) RequestVote(
	ctx context.Context,
	request *raftproto.RequestVoteRequest,
) (*raftproto.RequestVoteReply, error) {

	node.mu.Lock()
	defer node.mu.Unlock()

	log.Printf(
		"[Node %d] RequestVote received: candidate=%d term=%d lastLogIndex=%d lastLogTerm=%d currentTerm=%d",
		node.id,
		request.CandidateId,
		request.Term,
		request.LastLogIndex,
		request.LastLogTerm,
		node.currentTerm,
	)

	if request.Term > node.currentTerm {
		log.Printf(
			"[Node %d] Updating term: %d -> %d and becoming FOLLOWER",
			node.id,
			node.currentTerm,
			request.Term,
		)

		node.currentTerm = request.Term
		node.votedFor = -1
		node.leaderID = -1
		node.State = Follower
	}

	if request.Term < node.currentTerm {
		log.Printf(
			"[Node %d] Rejecting vote for candidate=%d: stale term %d < %d",
			node.id,
			request.CandidateId,
			request.Term,
			node.currentTerm,
		)

		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  false,
		}, nil
	}

	lastTerm := int64(0)

	if len(node.logEntries) > 0 {
		lastTerm = node.logEntries[len(node.logEntries)-1].Term
	}

	validLog :=
		request.LastLogTerm > lastTerm ||
			(request.LastLogTerm == lastTerm &&
				request.LastLogIndex >= int64(len(node.logEntries)-1))

	if !validLog {
		log.Printf(
			"[Node %d] Rejecting vote for candidate=%d: candidate log is outdated "+
				"(candidate last=(idx=%d,term=%d), follower last=(idx=%d,term=%d))",
			node.id,
			request.CandidateId,
			request.LastLogIndex,
			request.LastLogTerm,
			int64(len(node.logEntries)-1),
			lastTerm,
		)

		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  false,
		}, nil
	}

	if node.votedFor != -1 && node.votedFor != request.CandidateId {
		log.Printf(
			"[Node %d] Rejecting vote for candidate=%d: already voted for candidate=%d in term=%d",
			node.id,
			request.CandidateId,
			node.votedFor,
			node.currentTerm,
		)

		return &raftproto.RequestVoteReply{
			FollowerTerm: node.currentTerm,
			VoteGranted:  false,
		}, nil
	}

	node.votedFor = request.CandidateId
	node.resetElectionTimer()

	log.Printf(
		"[Node %d] GRANTED vote to candidate=%d for term=%d",
		node.id,
		request.CandidateId,
		node.currentTerm,
	)

	return &raftproto.RequestVoteReply{
		FollowerTerm: node.currentTerm,
		VoteGranted:  true,
	}, nil
}

func (node *RaftNode) AppendEntries(
	ctx context.Context,
	request *raftproto.AppendEntriesRequest,
) (*raftproto.AppendEntriesReply, error) {

	node.mu.Lock()
	defer node.mu.Unlock()

	log.Printf(
		"[Node %d] AppendEntries received: leader=%d term=%d prevLog=(idx=%d,term=%d) entries=%d leaderCommit=%d currentTerm=%d",
		node.id,
		request.LeaderId,
		request.Term,
		request.PrevLogIndex,
		request.PrevLogTerm,
		len(request.LogEntries),
		request.LeaderCommitIdx,
		node.currentTerm,
	)

	if request.Term < node.currentTerm {

		log.Printf(
			"[Node %d] AppendEntries REJECTED: stale leader term %d < current term %d",
			node.id,
			request.Term,
			node.currentTerm,
		)

		return &raftproto.AppendEntriesReply{
			Term:    node.currentTerm,
			Success: false,
		}, nil
	}

	if request.Term > node.currentTerm {

		log.Printf(
			"[Node %d] AppendEntries has newer term: %d -> %d. Becoming FOLLOWER",
			node.id,
			node.currentTerm,
			request.Term,
		)

		node.currentTerm = request.Term
		node.votedFor = -1
		node.State = Follower
	}

	node.leaderID = request.LeaderId

	if node.State != Follower {
		log.Printf(
			"[Node %d] Stepping down to FOLLOWER because leader=%d has valid term=%d",
			node.id,
			request.LeaderId,
			request.Term,
		)

		node.State = Follower
	}

	lastLogIdx := len(node.logEntries) - 1

	if request.PrevLogIndex > -1 {

		if request.PrevLogIndex > int64(lastLogIdx) {

			log.Printf(
				"[Node %d] AppendEntries REJECTED: PrevLogIndex=%d is beyond follower lastLogIndex=%d",
				node.id,
				request.PrevLogIndex,
				lastLogIdx,
			)

			return &raftproto.AppendEntriesReply{
				Success: false,
				Term:    node.currentTerm,
			}, nil
		}

		prevLogTermAtReceiver := node.logEntries[request.PrevLogIndex].Term

		if prevLogTermAtReceiver != request.PrevLogTerm {

			log.Printf(
				"[Node %d] AppendEntries REJECTED: PrevLogTerm mismatch at index=%d "+
					"(followerTerm=%d leaderTerm=%d)",
				node.id,
				request.PrevLogIndex,
				prevLogTermAtReceiver,
				request.PrevLogTerm,
			)

			return &raftproto.AppendEntriesReply{
				Success: false,
				Term:    node.currentTerm,
			}, nil
		}
	}

	for i, reqEntry := range request.LogEntries {

		followerIdx := int(request.PrevLogIndex) + 1 + i

		if followerIdx == len(node.logEntries) {

			log.Printf(
				"[Node %d] Appending %d new entries starting at index=%d",
				node.id,
				len(request.LogEntries)-i,
				followerIdx,
			)

			node.logEntries = append(
				node.logEntries,
				request.LogEntries[i:]...,
			)

			break
		}

		if node.logEntries[followerIdx].Term != reqEntry.Term {

			log.Printf(
				"[Node %d] Log conflict at index=%d: followerTerm=%d leaderTerm=%d. "+
					"Deleting entries from index=%d onward",
				node.id,
				followerIdx,
				node.logEntries[followerIdx].Term,
				reqEntry.Term,
				followerIdx,
			)

			node.logEntries = node.logEntries[:followerIdx]

			node.logEntries = append(
				node.logEntries,
				request.LogEntries[i:]...,
			)

			break
		}

		log.Printf(
			"[Node %d] Log entry already matches at index=%d term=%d",
			node.id,
			followerIdx,
			reqEntry.Term,
		)
	}

	if request.LeaderCommitIdx > node.commitIndex {

		oldCommitIndex := node.commitIndex

		node.commitIndex = min(
			request.LeaderCommitIdx,
			int64(len(node.logEntries)-1),
		)

		if node.commitIndex != oldCommitIndex {
			log.Printf(
				"[Node %d] commitIndex advanced: %d -> %d",
				node.id,
				oldCommitIndex,
				node.commitIndex,
			)
		}
	}

	node.resetElectionTimer()

	log.Printf(
		"[Node %d] AppendEntries ACCEPTED from leader=%d: logLength=%d commitIndex=%d",
		node.id,
		request.LeaderId,
		len(node.logEntries),
		node.commitIndex,
	)

	return &raftproto.AppendEntriesReply{
		Term:    node.currentTerm,
		Success: true,
	}, nil
}
