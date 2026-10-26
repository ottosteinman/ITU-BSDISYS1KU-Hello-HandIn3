package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	pb "chitchat/grpc"
	"chitchat/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to server
	conn, err := grpc.NewClient(
		"localhost:5000",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewChitChatClient(conn)

	// Ask user for name
	var username string
	fmt.Print("Enter your name: ")
	fmt.Scanln(&username)

	// Join chat
	stream, err := client.Join(
		context.Background(),
		&pb.JoinRequest{
			Name: username,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	logger.Log("Client", "join", username)

	// Start goroutine to listen for broadcasts
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}

			logger.LogReceive(msg.Sender, msg.Content, msg.LogicalTime)

			fmt.Printf(
				"[%d] %s: %s\n",
				msg.LogicalTime,
				msg.Sender,
				msg.Content,
			)
		}
	}()

	// Send messages
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		text := scanner.Text()

		if text == "leave" {
			break
		}

		logger.Log("Client", "send", username)

		client.Publish(
			context.Background(),
			&pb.PublishRequest{
				Sender:  username,
				Content: text,
			},
		)
	}

	logger.Log("Client", "leave", username)

	client.Leave(
		context.Background(),
		&pb.LeaveRequest{
			Name: username,
		},
	)
}
