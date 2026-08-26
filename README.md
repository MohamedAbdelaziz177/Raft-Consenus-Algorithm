# Raft-Consenus-Algorithm 🛢

A from-scratch implementation of the **Raft consensus algorithm** in Go, wired up to a small **replicated key-value store** with a gRPC transport layer, an HTTP API, and a live web dashboard for watching the cluster elect leaders and replicate writes in real time.

This project was built to understand Raft at the implementation level rather than just the paper — leader election, log replication, heartbeats, and commit-index advancement are all implemented and driven by real timers, goroutines, and mutex-guarded shared state across a 5-node cluster.

## Features

- **Leader election** with randomized election timeouts, term comparison, and vote-granting rules based on log up-to-dateness (`RequestVote` RPC)
- **Log replication** via `AppendEntries`, including conflict detection/resolution when a follower's log diverges from the leader's
- **Heartbeats** sent by the leader on a fixed ticker to suppress follower elections and keep the cluster stable
- **Commit index advancement** based on majority match-index agreement across peers
- **Safe state machine application** — committed log entries are streamed to an `ApplyCh` channel and applied asynchronously to the key-value store
- **HTTP API** (via [chi](https://github.com/go-chi/chi)) for `SET` / `GET` / `DELETE` operations, with automatic **"not leader" redirection** (`409` + `leaderId`) so clients always end up talking to the current leader
- **Live dashboard** (`raft-kv-dashboard.html`) that polls every node's `/raft/debug/state` endpoint to visualize term, commit index, last-applied index, and log length per node, and highlights the current leader

## Architecture

Each node runs as an independent process exposing:

- a **gRPC server** (`RaftServicesServer`) for internal `RequestVote` / `AppendEntries` traffic between peers
- an **HTTP server** for client-facing reads/writes and debug state

```
Client ──HTTP──▶ Handler (api) ──▶ RaftNode.Execute() ──▶ log append
                                         │
                                         ▼
                              gRPC AppendEntries to peers (leader only)
                                         │
                                         ▼
                         majority ack ──▶ commitIndex advances
                                         │
                                         ▼
                         StartMessageApplier ──▶ ApplyCh ──▶ KVStore
```

Node state transitions strictly follow Raft's `Follower → Candidate → Leader` model, protected end-to-end by a single mutex per node (`node.mu`) so term changes, vote bookkeeping, and log mutations never race.

## Tech Stack

- **Go** — core implementation
- **gRPC + Protobuf** — inter-node Raft RPCs (`RequestVote`, `AppendEntries`)
- **chi** — HTTP routing for the client-facing KV API
- **Vanilla HTML/CSS/JS** — the cluster dashboard

## Project Structure

```
raft/
├── api/
│   ├── handlers.go       # HTTP handlers (SET/GET/DELETE/debug) and leader-redirect logic
│   └── router.go         # chi route definitions
├── cmd/
│   └── server/
│       └── main.go       # Node entrypoint
├── docs/
├── proto/
│   ├── raft.proto        # RaftServices RPC + message definitions
│   ├── raft.pb.go        # Generated protobuf messages
│   └── raft_grpc.pb.go   # Generated gRPC client/server code
├── raft/
│   ├── raft.go           # RaftNode struct, state, NewRaftNode, Execute, RunServer
│   ├── election.go       # startElection, stepDownToFollower, levelUpToLeader
│   ├── heartbeats.go     # heartbeat ticker, election timeout detection, commit index advancement
│   └── rpc.go            # RequestVote / AppendEntries RPC handlers
├── storage/
│   └── kvstore.go        # KVStore + Command applied from committed log entries
├── go.mod
├── go.sum
└── raft-dashboard.html   # Live cluster visualizer
```

## Getting Started

```bash
# clone
git clone https://github.com/MohamedAbdelaziz177/Raft-Consenus-Algorithm.git
cd Raft-Consenus-Algorithm

# starts all 5 nodes in a single process — no flags needed
go run ./cmd/server
```

`cmd/server/main.go` hardcodes each node's Raft (gRPC) and HTTP API addresses and boots the whole cluster at once, so there's nothing to configure per-node.

> **Known issue:** node 0 is currently addressed as `localhost:5005` / `localhost:6005` while nodes 1–4 follow the `500X` / `600X` pattern — likely a typo for `5000` / `6000`. The dashboard hardcodes node 0's HTTP port as `6000`, so until this is fixed, node 0 will show as offline in the dashboard.

Then open `raft-dashboard.html` in a browser to watch the cluster elect a leader and serve requests.

## API

| Method | Path             | Description                                  |
|--------|------------------|-----------------------------------------------|
| POST   | `/raft/set`      | `{ "key": "...", "value": "..." }` — appends a SET command to the leader's log |
| GET    | `/raft/{key}`    | Reads a key from the local KV store           |
| DELETE | `/raft/{key}`    | Appends a DELETE command to the leader's log  |
| GET    | `/raft/debug/state` | Returns `{ state, term, commitIndex, lastApplied, logLength }` for the node |

Any write sent to a non-leader node returns `409` with a `leaderId` field so the client can retry against the correct node — the dashboard handles this redirect automatically.

## What I Learned

This project was as much about learning idiomatic Go concurrency as it was about Raft itself:

- **Goroutines** — fanning out `RequestVote`/`AppendEntries` RPCs to every peer concurrently and joining on a `sync.WaitGroup`
- **Channels** — decoupling log commitment from state-machine application via `ApplyCh`, so committed entries are applied without blocking the replication path
- **Mutexes** — guarding all shared node state (`currentTerm`, `votedFor`, `logEntries`, `commitIndex`, …) against concurrent access from RPC handlers, the election loop, and the heartbeat loop
- **Timers & Tickers** — implementing randomized election timeouts with `time.Timer` (and safely draining/resetting them) alongside a fixed-interval `time.Ticker` for leader heartbeats

## Future Work

- **Persistence** — the key-value store and Raft log currently live entirely in memory; a planned next step is persisting both to disk so a node can restart and recover its committed state instead of starting from empty.
