package raft

import "log"

// Debugging
const Debug = false
const ElectionDebug = false
const HeartbeatDebug = false
const AppendEntriesDebug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ElectionPrintf(format string, a ...interface{}) (n int, err error) {
	if ElectionDebug {
		log.Printf("Election - "+format, a...)
	}
	return
}

func HeartbeatPrintf(format string, a ...interface{}) (n int, err error) {
	if HeartbeatDebug {
		log.Printf(format, a...)
	}
	return
}

func AppendEntriesPrintf(format string, a ...interface{}) (n int, err error) {
	if AppendEntriesDebug {
		log.Printf(format, a...)
	}
	return
}
