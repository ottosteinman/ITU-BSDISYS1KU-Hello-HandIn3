# Chit Chat — a distributed chat service in Go

Chit Chat is a small distributed chat service used to explore communication,
coordination and the ordering of events in a system without a single shared
clock. One **service process** fans out events to any number of **client
processes**. All communication uses **gRPC** with **Protocol Buffers**
(`grpc/chit_chat.proto`), and all logical timestamps are **Lamport
timestamps** computed by the service.

The repository layout follows the assignment spec:

```
chit-chat/
├── client/    # client process (one per participant)
├── grpc/      # .proto file + generated Go bindings
├── server/    # service process (the only server)
├── logs/      # captured run demonstrating the requirements (S5–S7)
├── go.mod / go.sum
└── readme.md  # this file
```

## Requirements & dependencies

- Go ≥ 1.24 (built and tested with 1.27)
- `protoc` **only if** you change the `.proto` file — the generated
  bindings in `grpc/` are committed, so you can build without protoc.

## Running

### 1. Start the service (one process)

```bash
go run ./server            # listens on :50051
# or with a custom port:
CHITCHAT_PORT=6000 go run ./server
```

You should see:

```
2026/10/25 17:21:07 component=Server event=server_startup port=50051
```

### 2. Start at least three clients (separate processes, one terminal each)

```bash
go run ./client Alice
go run ./client Bob localhost:50051
go run ./client Carol
```

Each client prints its name prompt, then reads commands from stdin:

| input    | effect                                          |
|----------|-------------------------------------------------|
| `<text>` | publish `text` (must be valid UTF-8, ≤ 128 chars) |
| `leave`  | leave the chat (orderly Leave RPC) and exit      |
| Ctrl-D   | same as `leave`                                  |

Try, in Alice's terminal:

```
hello everyone
```

You will see (on **every** client, including Alice's own):

```
[t=2] Participant Alice joined Chit Chat at logical time 1
[t=2] Alice: hello everyone
```

### 3. Stop everything

`leave` (or Ctrl-D) in each client, then Ctrl-C in the server terminal.
The server shuts down gracefully and logs `server_shutdown`.

## Running three concurrent clients (the S2 minimum)

Open one terminal per client so all three are active at once, e.g. Alice,
Bob and Carol. The captured run in `logs/` is the evidence for the hand-in:
it shows three concurrent clients, at least one join and one leave,
broadcast delivery to all active participants, and the full structured log
trail. The same sequence can be reproduced by hand:

```
# 1) server in one terminal
go run ./server

# 2) then three concurrent clients in three more terminals
go run ./client Alice
go run ./client Bob localhost:50051
go run ./client Carol
```

Type a message in any client, or `leave` to exit — every broadcast (joins,
leaves, chat) appears in each active client with its logical timestamp.

## Regenerating the gRPC bindings (only if you edit the .proto)

```bash
cd grpc
protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. chit_chat.proto
```

## Design in one paragraph (details in the report)

- **Topology:** client–server (one service, many clients). The service is
  the single fan-out point; clients only talk to the service.
- **Streaming:** the `Join` RPC is **server-side streaming** — a client
  calls it once and receives a long-lived stream of broadcasts; `Publish`
  and `Leave` are unary. No client-side or bidirectional streaming is
  needed because clients never send a continuous flow.
- **Concurrency:** each client connection is served by a dedicated
  goroutine (its Join handler + its reader goroutine). The service
  broadcasts into each participant's private buffered channel with
  non-blocking sends, so a slow or dead client can never block delivery to
  the others.
- **Timestamps:** Lamport. The service keeps the max of all timestamps
  seen so far (its own or carried in by clients); each event takes
  `T = max(local, carried) + 1`. Clients carry their own clock in
  `PublishRequest.lamport` / `LeaveRequest.lamport`, so ordering stays
  causal even when multiple clients publish in quick succession.
- **Logging:** standard `log` library, structured lines
  `timestamp component={Server|Client} event=... client=<id> time=<n>`
  covering server startup/shutdown, connect/disconnect, join/leave
  broadcasts, chat broadcasts and per-recipient delivery.

## Verification (what the captured `logs/` demonstrate)

- **S2** — server + 3 concurrent client processes (`client_connected` x3).
- **S3** — `publish` enforces valid UTF-8 and ≤ 128 chars server-side.
- **S4** — every `broadcast_chat` lists `delivered=N` = active clients.
- **S5** — `broadcast_join` with `"Participant X joined Chit Chat at
  logical time L"` delivered to all, **including X** (delivered=1 for
  Alice's own join).
- **S6** — `broadcast_leave` with `"Participant X left Chit Chat at
  logical time L"` delivered to all remaining.
- **S7** — every client log shows the broadcast content and its logical
  timestamp, both displayed (`[t=N] ...`) and logged (`message_received`).
- **Lamport** — logical time strictly increases: 1,2,3,...,14 in the demo.
