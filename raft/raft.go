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
	raftproto.UnimplementedRaftServicesServer

	id    int32
	state raftState

	mu sync.Mutex

	currentTerm int64
	votedFor    int32

	logEntries []*raftproto.LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[int]int64
	matchIndex map[int]int64

	server *raftproto.RaftServicesServer
	peers  map[int]raftproto.RaftServicesClient

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

func (node *RaftNode) RunServer(addr string) error {

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	raftproto.RegisterRaftServicesServer(grpcServer, *node.server)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc server failed: %v", err)
			panic(err)
		}
	}()

	return nil
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
