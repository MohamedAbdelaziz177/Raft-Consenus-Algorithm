package raft

import (
	"sync"
	"time"
)

type raftState int

const (
	Follower raftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command []byte
}

type ApplyMsg struct {
	CommandValid bool
	Command      string
	CommandIndex int
}

type RaftNode struct {
	id    int
	state raftState

	mu sync.Mutex

	currentTerm int64
	votedFor    int

	logEntries []LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[int]int64
	matchIndex map[int]int64

	peers []string

	applyCh chan ApplyMsg

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
}
