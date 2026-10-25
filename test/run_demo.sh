#!/usr/bin/env bash
#
# run_demo.sh — starts one Chit Chat server and three clients (Alice, Bob,
# Carol), drives a scripted join / publish / leave sequence, and captures
# the server + client logs. The logs in ./logs demonstrate that the
# system meets S5-S7 and the logging requirements.
#
# Usage: bash test/run_demo.sh
#
set -u
cd "$(dirname "$0")/.."

BIN_DIR="$(pwd)/bin"
LOG_DIR="$(pwd)/logs"
mkdir -p "$BIN_DIR" "$LOG_DIR"

echo ">> building"
go build -o "$BIN_DIR/server" ./server || exit 1
go build -o "$BIN_DIR/client" ./client || exit 1

PORT="${CHITCHAT_PORT:-50051}"
export CHITCHAT_PORT="$PORT"

# stop a stale server from a previous run
pkill -f "bin/server" 2>/dev/null
sleep 0.5

echo ">> starting server on :$PORT"
CHITCHAT_PORT="$PORT" "$BIN_DIR/server" > "$LOG_DIR/server.log" 2>&1 &
SERVER_PID=$!

# wait for the port to accept connections
for i in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    exec 3>&- 3<&-
    break
  fi
  sleep 0.2
done

# run_client NAME <scripted lines...>
# Each line is fed to the client's stdin with a short pause between lines;
# when stdin closes the client leaves (orderly Leave RPC) and exits.
run_client() {
  local name="$1"; shift
  (
    for line in "$@"; do
      printf '%s\n' "$line"
      sleep 1.2
    done
  ) | "$BIN_DIR/client" "$name" "127.0.0.1:$PORT" > "$LOG_DIR/client-$name.out" 2> "$LOG_DIR/client-$name.log"
}

echo ">> starting clients (Alice, Bob, Carol)"
run_client Alice "hello everyone" "this is alice" "leaving for now" &
A=$!
sleep 0.6
run_client Bob "hi alice, hi carol" "bob here" &
B=$!
sleep 0.6
run_client Carol "ciao" "carol says hi" "bye" &
C=$!

wait $A $B $C
echo ">> clients finished"

echo ">> stopping server"
kill -INT "$SERVER_PID" 2>/dev/null
wait "$SERVER_PID" 2>/dev/null
echo ">> done. Logs in $LOG_DIR"
echo
echo "=== server.log (tail) ==="
tail -n 25 "$LOG_DIR/server.log"
