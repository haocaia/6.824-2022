package raft

import "log"

// Debugging
const Debug = true
const DEFAULT_SLEEP_MS = 50

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
		log.Println()
	}
	return
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isOutDate(argsTerm, term int) bool {
	return argsTerm < term
}
