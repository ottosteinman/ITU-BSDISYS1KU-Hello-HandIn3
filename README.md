# Chit Chat

Distributed chat service for Hand-In 3: one Go server process and multiple Go client processes, communicating over gRPC.

## Run

Terminal 1 — start the server (listens on port 5000):

```sh
go run ./server
```

Terminal 2, 3 — start the clients:

```sh
go run ./client
```

Each client asks for a name, then every line you type is broadcast to all participants. Messages are limited to 128 characters. Type `leave` to exit.

Stop the server with Ctrl+C.

## Logs

All events (join/leave, message delivery, startup/shutdown) are printed to stderr and appended to `chitchat.log` in the working directory.
