package raft

import (
	"math/rand"
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

func NewRaftNode() *RaftNode {
	return &RaftNode{
		state:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		logEntries:      make([]LogEntry, 0),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make(map[int]int64),
		matchIndex:      make(map[int]int64),
		applyCh:         make(chan ApplyMsg),
		electionTimer:   time.NewTimer(time.Duration(rand.Intn(400)) * time.Millisecond),
		heartbeatTicker: time.NewTicker(50 * time.Millisecond),
	}
}
