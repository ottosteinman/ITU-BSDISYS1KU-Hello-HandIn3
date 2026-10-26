package logger

import "log"

func Log(component, action, username string) {
	log.Printf("component=%s action=%s client=%s", component, action, username)
}
