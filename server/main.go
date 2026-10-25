package main

import (
	"sync"

	pb "chitchat/grpc"
)

type Server struct {
	pb.UnimplementedChitChatServer

	mu      sync.Mutex
	clients map[string]pb.ChitChat_JoinServer
	lamport int64
}

func (s *Server) Join(req *pb.JoinRequest, stream pb.ChitChat_JoinServer) error {
	return nil
}
