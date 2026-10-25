// Package main implements the Chit Chat service process (server).
//
// Concurrency model (goroutines + channels, as required):
//
//   - The service owns a hub: the set of active participants, the Lamport
//     clock, and the fan-out.
//   - Each client connection is served by a DEDICATED goroutine (the
//     reader) that drains that participant's buffered channel and forwards
//     every broadcast to the client over the gRPC stream.
//   - Every participant has its OWN buffered channel. broadcast() does a
//     NON-BLOCKING send into each channel. Because the channel is
//     buffered and the send never blocks, a slow or dead client can never
//     stall delivery to the others (spec: "without blocking message
//     delivery"). If a channel's buffer is full we treat the participant
//     as unresponsive and remove it.
//   - Leave handling is exactly-once: hub.remove() reports whether the
//     participant was actually removed, so an orderly Leave and a
//     simultaneous disconnect cannot both broadcast a leave.
//
// Structured logging uses the standard log package; every line carries a
// timestamp plus component (Server), event type and identifiers:
//
//	2026-10-25 17:12:03 component=Server event=broadcast_join client=Alice time=3 delivered=2
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unicode/utf8"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	maxMessageChars = 128
	sendBufSize     = 256 // per-participant outbound buffer
)

// participant is one active client.
type participant struct {
	name string
	// send is the participant's private outbound buffer. broadcast() does
	// a non-blocking send into it; the dedicated reader goroutine drains
	// it onto the gRPC stream. It is never closed (a send to a closed
	// channel panics); the participant is dropped by removing it from the
	// hub and closing done.
	send chan *pb.BroadcastMessage
	// done is closed when the participant is removed; the reader goroutine
	// exits as soon as it observes this.
	done     chan struct{}
	doneOnce sync.Once
}

func (p *participant) markDone() {
	p.doneOnce.Do(func() { close(p.done) })
}

// hub is the concurrency core of the service process.
type hub struct {
	mu   sync.Mutex
	part map[string]*participant

	// clock is the service's Lamport clock: the maximum of all timestamps
	// seen so far (locally generated or carried in by clients).
	clock int64

	log *logger
}

func newHub(l *logger) *hub {
	return &hub{part: make(map[string]*participant), log: l}
}

// tick implements the Lamport rule: T = max(local, carried) + 1.
func (h *hub) tick(carried int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if carried > h.clock {
		h.clock = carried
	}
	h.clock++
	return h.clock
}

// add registers a new participant and returns it. ok is false if the name
// is already active.
func (h *hub) add(name string) (*participant, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.part[name]; exists {
		return nil, false
	}
	p := &participant{
		name: name,
		send: make(chan *pb.BroadcastMessage, sendBufSize),
		done: make(chan struct{}),
	}
	h.part[name] = p
	return p, true
}

// remove drops a participant and signals its reader to exit. It returns
// true if the participant was present (and thus removed exactly once).
func (h *hub) remove(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	p, ok := h.part[name]
	if !ok {
		return false
	}
	delete(h.part, name)
	p.markDone()
	return true
}

// active reports whether name is currently a registered participant.
func (h *hub) active(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.part[name]
	return ok
}

// broadcast delivers b to every active participant using NON-BLOCKING
// sends into their private buffers. It returns how many participants
// accepted the message and the names of any whose buffer was full (they
// are considered unresponsive and should be dropped by the caller).
func (h *hub) broadcast(b *pb.BroadcastMessage) (int, []string) {
	h.mu.Lock()
	receivers := make([]*participant, 0, len(h.part))
	for _, p := range h.part {
		receivers = append(receivers, p)
	}
	h.mu.Unlock()

	delivered := 0
	var unresponsive []string
	for _, p := range receivers {
		select {
		case p.send <- b:
			delivered++
		default:
			// Buffer full: the reader cannot keep up. Drop it.
			unresponsive = append(unresponsive, p.name)
		}
	}
	return delivered, unresponsive
}

// server implements the generated ChitChatServer interface.
type server struct {
	pb.UnimplementedChitChatServer
	hub *hub
	log *logger
}

// dropUnresponsive removes (and leaves) participants whose buffer filled
// up during a broadcast.
func (s *server) dropUnresponsive(names []string) {
	for _, n := range names {
		s.log.Event("client_unresponsive", "client="+n)
		go s.depart(n, 0, "unresponsive")
	}
}

// depart removes a participant and broadcasts the leave notification.
// It is safe to call concurrently: only the first caller actually leaves.
// It returns the leave timestamp (0 if the participant was already gone).
func (s *server) depart(name string, carried int64, cause string) int64 {
	if !s.hub.remove(name) {
		return 0
	}
	s.log.Event("client_disconnected", "client="+name, "cause="+cause)

	lt := s.hub.tick(carried)
	leave := &pb.BroadcastMessage{
		Sender:      name,
		Content:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d", name, lt),
		LogicalTime: lt,
		Kind:        pb.BroadcastKind_LEAVE,
	}
	delivered, unresponsive := s.hub.broadcast(leave)
	s.dropUnresponsive(unresponsive)
	s.log.Event("broadcast_leave", "client="+name, "time="+strconv.FormatInt(lt, 10), "delivered="+strconv.Itoa(delivered))
	return lt
}

// Join registers the participant and keeps the RPC open as a
// server-side stream. The DEDICATED goroutine for this connection is the
// reader below: it stays alive for the lifetime of the participation and
// forwards every broadcast to the client. It is started BEFORE the join
// broadcast so the newcomer also receives its own join message (S5).
func (s *server) Join(req *pb.JoinRequest, stream pb.ChitChat_JoinServer) error {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return errors.New("join: empty participant name")
	}

	p, ok := s.hub.add(name)
	if !ok {
		return fmt.Errorf("join: participant %q is already active", name)
	}
	s.log.Event("client_connected", "client="+name)

	// Dedicated reader goroutine for this connection: drain the
	// participant's buffer onto the gRPC stream.
	go func() {
		for {
			select {
			case <-p.done:
				return
			case msg, open := <-p.send:
				if !open {
					return
				}
				s.log.Event("message_delivered", "client="+name, "kind="+msg.GetKind().String(), "time="+strconv.FormatInt(msg.GetLogicalTime(), 10))
				if err := stream.Send(msg); err != nil {
					// The client went away (crash / network drop).
					s.depart(name, 0, "stream_error:"+err.Error())
					return
				}
			}
		}
	}()

	// Join broadcast (S5). The newcomer is already registered, so it is a
	// recipient too.
	lt := s.hub.tick(0)
	join := &pb.BroadcastMessage{
		Sender:      name,
		Content:     fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", name, lt),
		LogicalTime: lt,
		Kind:        pb.BroadcastKind_JOIN,
	}
	delivered, unresponsive := s.hub.broadcast(join)
	s.dropUnresponsive(unresponsive)
	s.log.Event("broadcast_join", "client="+name, "time="+strconv.FormatInt(lt, 10), "delivered="+strconv.Itoa(delivered))

	// Keep the RPC open until the client leaves or the connection dies.
	<-stream.Context().Done()
	s.depart(name, 0, stream.Context().Err().Error())
	return nil
}

// Publish validates and broadcasts a chat message.
func (s *server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	name := strings.TrimSpace(req.GetName())
	msg := strings.TrimSpace(req.GetMessage())

	if name == "" {
		return nil, errors.New("publish: empty participant name")
	}
	if !utf8.ValidString(msg) {
		return nil, errors.New("publish: message is not valid UTF-8")
	}
	if n := len([]rune(msg)); n > maxMessageChars {
		return nil, fmt.Errorf("publish: message is %d characters, maximum is %d", n, maxMessageChars)
	}
	if !s.hub.active(name) {
		return nil, fmt.Errorf("publish: participant %q is not active", name)
	}

	lt := s.hub.tick(req.GetLamport())
	b := &pb.BroadcastMessage{
		Sender:      name,
		Content:     fmt.Sprintf("%s: %s", name, msg),
		LogicalTime: lt,
		Kind:        pb.BroadcastKind_CHAT,
	}
	delivered, unresponsive := s.hub.broadcast(b)
	s.dropUnresponsive(unresponsive)
	s.log.Event("broadcast_chat", "client="+name, "time="+strconv.FormatInt(lt, 10), "chars="+strconv.Itoa(len([]rune(msg))), "delivered="+strconv.Itoa(delivered))

	return &pb.PublishResponse{LogicalTime: lt}, nil
}

// Leave handles an orderly departure.
func (s *server) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	name := strings.TrimSpace(req.GetName())
	lt := s.depart(name, req.GetLamport(), "leave")
	return &pb.LeaveResponse{LogicalTime: lt}, nil
}

func main() {
	log := newLogger("Server")

	port := os.Getenv("CHITCHAT_PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("server_startup_failed", "error="+err.Error())
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChitChatServer(grpcServer, &server{hub: newHub(log), log: log})

	// Standard gRPC health service, so clients can verify the node is up.
	hs := health.NewServer()
	hs.SetServingStatus("chitchat", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hs)

	log.Event("server_startup", "port="+port)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Event("server_shutdown", "reason=signal", "signal="+sig.String())
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("server_serve_failed", "error="+err.Error())
	}
	log.Event("server_shutdown_complete")
}
