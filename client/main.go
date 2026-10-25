// Package main implements a Chit Chat client process.
//
// A client is a separate process from the server and from any other
// client. It joins once (server-side stream stays open), then runs an
// interactive loop reading commands from stdin:
//
//	  <text>     publish "text" (up to 128 chars)
//	  leave      leave the conversation and exit
//	  quit       same as leave (alias)
//
// Every broadcast received (chat, join, leave) is displayed on the
// client interface AND logged with its logical timestamp, as required by
// S7 and the logging spec.
//
// The client keeps its own Lamport clock: on every receive and send it
// updates T = max(local, carried) and reports T back to the service, so
// the service can compute a causally consistent timestamp.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxMessageChars = 128
)

var leaveRe = regexp.MustCompile(`^(leave|quit|exit|q)$`)

func main() {
	// name and address come from argv: client <name> [addr]
	name := "client"
	addr := "localhost:50051"
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) != "" {
		name = strings.TrimSpace(os.Args[1])
	}
	if len(os.Args) > 2 {
		addr = os.Args[2]
	}

	l := newLogger("Client")
	l.Event("client_startup", "client="+name, "server="+addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		l.Fatal("client_connect_failed", "error="+err.Error())
	}
	defer conn.Close()

	client := pb.NewChitChatClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Join (server-side stream) -------------------------------------
	stream, err := client.Join(ctx, &pb.JoinRequest{Name: name})
	if err != nil {
		l.Fatal("client_join_failed", "client="+name, "error="+err.Error())
	}
	l.Event("client_joined", "client="+name)

	// clock is the client's Lamport clock (max timestamp seen so far).
	var clock int64
	bump := func(carried int64) int64 {
		if carried > clock {
			clock = carried
		}
		return clock
	}

	// --- Broadcast receiver goroutine (dedicated, one per client) ------
	receiverDone := make(chan error, 1)
	go func() {
		defer close(receiverDone)
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				receiverDone <- nil
				return
			}
			if err != nil {
				receiverDone <- err
				return
			}
			bump(msg.GetLogicalTime())
			// S7.i: display; S7.ii: log content + logical timestamp.
			l.Printf("[t=%d] %s", msg.GetLogicalTime(), msg.GetContent())
			l.Event("message_received", "client="+name, "kind="+msg.GetKind().String(), "time="+strconv.FormatInt(msg.GetLogicalTime(), 10))
		}
	}()

	// --- Interactive command loop --------------------------------------
	fmt.Printf("You are %q in Chit Chat (server %s). Type a message, or 'leave'.\n", name, addr)
	stdin := bufio.NewScanner(os.Stdin)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	joined := true
	for {
		select {
		case err := <-receiverDone:
			if err != nil {
				l.Event("client_stream_closed", "client="+name, "cause="+err.Error())
			}
			joined = false
			continue
		case sig := <-sigCh:
			l.Event("client_signal", "client="+name, "signal="+sig.String())
			return
		default:
		}

		if !stdin.Scan() {
			break // stdin closed (Ctrl-D / pipe exhausted)
		}
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}
		if leaveRe.MatchString(line) {
			break
		}

		if joined {
			if !utf8.ValidString(line) {
				fmt.Println("error: message is not valid UTF-8")
				continue
			}
			if n := len([]rune(line)); n > maxMessageChars {
				fmt.Printf("error: message is %d characters, maximum is %d\n", n, maxMessageChars)
				continue
			}
			resp, err := client.Publish(ctx, &pb.PublishRequest{
				Name:    name,
				Message: line,
				Lamport: clock,
			})
			if err != nil {
				l.Event("client_publish_failed", "client="+name, "error="+err.Error())
				fmt.Println("error: " + err.Error())
				continue
			}
			bump(resp.GetLogicalTime())
		}
	}

	// --- Leave ----------------------------------------------------------
	if joined {
		_, err := client.Leave(ctx, &pb.LeaveRequest{Name: name, Lamport: clock})
		if err != nil {
			l.Event("client_leave_failed", "client="+name, "error="+err.Error())
		}
	}
	l.Event("client_shutdown", "client="+name)
}
