// Package main implements the Chit Chat service process (server).
//
// Structured logging helper: wraps the standard log package so every line
// carries a timestamp plus component, event type and identifiers, e.g.:
//
//	2026-10-25 17:12:03 component=Server event=server_startup port=50051
//
// The standard library log package is used as required by the spec.

package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

type logger struct {
	component string
	std       *log.Logger
}

func newLogger(component string) *logger {
	return &logger{
		component: component,
		std:       log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Event logs one structured event, e.g.:
//
//	component=Server event=broadcast_join client=Alice time=3
func (l *logger) Event(event string, kv ...string) {
	line := fmt.Sprintf("component=%s event=%s", l.component, event)
	if len(kv) > 0 {
		line += " " + strings.Join(kv, " ")
	}
	l.std.Println(line)
}

// Printf is a pass-through for free-form output (still timestamped).
func (l *logger) Printf(format string, args ...any) {
	l.std.Printf(format, args...)
}

// Fatal logs the event and exits with status 1.
func (l *logger) Fatal(event string, kv ...string) {
	l.Event(event, kv...)
	os.Exit(1)
}
