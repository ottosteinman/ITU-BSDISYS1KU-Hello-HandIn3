// Structured logging helper for the client process. Wraps the standard
// log package so every line carries a timestamp plus component, event
// type and identifiers, e.g.:
//
//	2026-10-25 17:12:03 component=Client event=message_received client=Alice kind=CHAT time=3
//
// Mirrors the server's logger so both components log in the same
// structured format (timestamp, component, event, identifiers).

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
//	component=Client event=message_received client=Alice kind=CHAT time=3
func (l *logger) Event(event string, kv ...string) {
	line := fmt.Sprintf("component=%s event=%s", l.component, event)
	if len(kv) > 0 {
		line += " " + strings.Join(kv, " ")
	}
	l.std.Println(line)
}

// Printf is a pass-through for free-form output (still timestamped) —
// used for the S7 display of received broadcasts.
func (l *logger) Printf(format string, args ...any) {
	l.std.Printf(format, args...)
}

// Fatal logs the event and exits with status 1.
func (l *logger) Fatal(event string, kv ...string) {
	l.Event(event, kv...)
	os.Exit(1)
}
