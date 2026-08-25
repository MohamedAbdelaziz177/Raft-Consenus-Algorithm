package raft

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	raftproto "github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type raftState int

const (
	Follower raftState = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
}

type PeerClient struct {
	ID         string
	Address    string
	GrpcClient raftproto.RaftServicesClient
	Connection *grpc.ClientConn
}

type IRaftNode interface {
	execute(command []byte) (int64, bool)
}

type RaftNode struct {
	raftproto.UnimplementedRaftServicesServer

	id    int32
	State raftState

	mu sync.Mutex

	currentTerm int64
	votedFor    int32

	logEntries []*raftproto.LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[int]int64
	matchIndex map[int]int64

	peers map[int]raftproto.RaftServicesClient

	ApplyCh chan ApplyMsg

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
}

func NewRaftNode() *RaftNode {
	return &RaftNode{
		State:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		logEntries:      make([]*raftproto.LogEntry, 0),
		commitIndex:     -1,
		lastApplied:     -1,
		nextIndex:       make(map[int]int64),
		matchIndex:      make(map[int]int64),
		ApplyCh:         make(chan ApplyMsg),
		electionTimer:   time.NewTimer(time.Duration(rand.Intn(400)) * time.Millisecond),
		heartbeatTicker: nil,
	}
}

func (node *RaftNode) RunServer(addr string) error {

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	raftproto.RegisterRaftServicesServer(grpcServer, node)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc server failed: %v", err)
			panic(err)
		}
	}()

	return nil
}

func (node *RaftNode) Execute(command []byte) (int64, bool) {

	node.mu.Lock()
	node.mu.Unlock()

	if node.State != Leader {
		return -1, false
	}

	incomingIdx := int64(len(node.logEntries))
	entry := raftproto.LogEntry{
		Data: command,
		Idx:  incomingIdx,
		Term: node.currentTerm,
	}

	node.logEntries = append(node.logEntries, &entry)

	return incomingIdx, true
}

func (node *RaftNode) RegisterClients(peerAddrs map[int]string) {
	clients := make(map[int]raftproto.RaftServicesClient)

	for k, v := range peerAddrs {
		conn, err := grpc.NewClient(
			v, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			log.Printf("failed to dial peer %d: %v", k, err)
			continue
		}

		clients[k] = raftproto.NewRaftServicesClient(conn)
	}

	node.peers = clients
}

func (node *RaftNode) StartMessageApplier() {
	go func() {
		for {
			node.mu.Lock()

			if node.lastApplied >= node.commitIndex {
				node.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				continue
			}

			node.lastApplied++

			idx := node.lastApplied
			entry := node.logEntries[idx]

			msg := ApplyMsg{
				Command:      entry.Data,
				CommandValid: true,
				CommandIndex: int(idx),
			}

			node.mu.Unlock()

			node.ApplyCh <- msg
		}
	}()
}
