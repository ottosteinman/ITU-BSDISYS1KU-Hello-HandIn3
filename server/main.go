package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"unicode/utf8"

	pb "chitchat/grpc"
	"chitchat/logger"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedChitChatServer

	mu      sync.Mutex
	clients map[string]pb.ChitChat_JoinServer
	lamport int64
}

func (s *Server) Join(req *pb.JoinRequest, stream pb.ChitChat_JoinServer) error {

	logger.Log("Server", "join", req.Name)

	s.mu.Lock()
	s.clients[req.Name] = stream
	s.lamport++

	msg := &pb.BroadcastMessage{
		Sender:      req.Name,
		Content:     fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", req.Name, s.lamport),
		LogicalTime: s.lamport,
		Kind:        pb.BroadcastKind_JOIN,
	}

	s.mu.Unlock()

	s.Broadcast(msg)

	<-stream.Context().Done()

	s.mu.Lock()
	left := s.clients[req.Name] == stream
	if left {
		delete(s.clients, req.Name)
		s.lamport++
		msg = &pb.BroadcastMessage{
			Sender:      req.Name,
			Content:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d", req.Name, s.lamport),
			LogicalTime: s.lamport,
			Kind:        pb.BroadcastKind_LEAVE,
		}
	}
	s.mu.Unlock()

	if left {
		logger.Log("Server", "disconnect", req.Name)
		s.Broadcast(msg)
	}

	return nil
}

func (s *Server) Broadcast(msg *pb.BroadcastMessage) {
	s.mu.Lock()
	clients := make([]pb.ChitChat_JoinServer, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	for _, client := range clients {
		client.Send(msg)
	}
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {

	logger.Log("Server", "send", req.Sender)

	if utf8.RuneCountInString(req.Content) > 128 {
		return nil, fmt.Errorf("message too long")
	}

	s.mu.Lock()

	s.lamport++

	msg := &pb.BroadcastMessage{
		Sender:      req.Sender,
		Content:     req.Content,
		LogicalTime: s.lamport,
		Kind:        pb.BroadcastKind_CHAT,
	}

	s.mu.Unlock()

	s.Broadcast(msg)

	return &pb.PublishResponse{}, nil
}

func (s *Server) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {

	logger.Log("Server", "leave", req.Name)

	s.mu.Lock()

	delete(s.clients, req.Name)

	s.lamport++

	msg := &pb.BroadcastMessage{
		Sender:      req.Name,
		Content:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d", req.Name, s.lamport),
		LogicalTime: s.lamport,
		Kind:        pb.BroadcastKind_LEAVE,
	}

	s.mu.Unlock()

	s.Broadcast(msg)

	return &pb.LeaveResponse{}, nil
}

func main() {

	s := grpc.NewServer()

	pb.RegisterChitChatServer(s, &Server{
		clients: make(map[string]pb.ChitChat_JoinServer),
	})

	lis, err := net.Listen("tcp", ":5000")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Server started on :5000")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		logger.Log("Server", "shutdown", "")
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
