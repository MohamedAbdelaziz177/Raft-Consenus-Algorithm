package raft

import (
	"math/rand"
	"sync"
	"time"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
	"google.golang.org/grpc"
)

type raftState int

const (
	Follower raftState = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      string
	CommandIndex int
}

type PeerClient struct {
	ID         string
	Address    string
	GrpcClient raftproto.RaftServicesClient
	Connection *grpc.ClientConn
}

type RaftNode struct {
	id    int
	state raftState

	mu sync.Mutex

	currentTerm int64
	votedFor    int

	logEntries []*raftproto.LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[int]int64
	matchIndex map[int]int64

	peers map[int]*PeerClient

	applyCh chan ApplyMsg

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
}

func NewRaftNode() *RaftNode {
	return &RaftNode{
		state:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		logEntries:      make([]*raftproto.LogEntry, 0),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make(map[int]int64),
		matchIndex:      make(map[int]int64),
		applyCh:         make(chan ApplyMsg),
		electionTimer:   time.NewTimer(time.Duration(rand.Intn(400)) * time.Millisecond),
		heartbeatTicker: time.NewTicker(150 * time.Millisecond),
	}
}
