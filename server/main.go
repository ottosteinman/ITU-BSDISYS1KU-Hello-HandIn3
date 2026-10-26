package main

import (
	"context"
	"log"
	"net"
	"sync"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedChitChatServer

	mu      sync.Mutex
	clients map[string]pb.ChitChat_JoinServer
	lamport int64
}

func (s *Server) Join(req *pb.JoinRequest, stream pb.ChitChat_JoinServer) error {

	s.mu.Lock()
	s.clients[req.Name] = stream
	s.lamport++

	msg := &pb.BroadcastMessage{
		Sender:      req.Name,
		Content:     req.Name + " joined the chat",
		LogicalTime: s.lamport,
		Kind:        pb.BroadcastKind_JOIN,
	}

	s.mu.Unlock()

	s.Broadcast(msg)

	<-stream.Context().Done()

	return nil
}

func (s *Server) Broadcast(msg *pb.BroadcastMessage) {
	for _, client := range s.clients {
		client.Send(msg)
	}
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
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

	s.mu.Lock()

	delete(s.clients, req.Name)

	s.lamport++

	msg := &pb.BroadcastMessage{
		Sender:      req.Name,
		Content:     req.Name + " left the chat",
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

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
