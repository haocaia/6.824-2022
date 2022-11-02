package raft

import (
	"errors"
	"fmt"
)

// Log
/* Command: 指令
 * CurrentTerm: 当前log所在term
 * CurrentIndex: 当前log的index, 从1开始增加
*/

// 0号Entry
var zeroEntry = Entry{
	Command:      -1,
	CurrentTerm:  -1,
	CurrentIndex: 0,
}

type Log struct {
	Entries []Entry
}

type Entry struct {
	Command      int
	CurrentTerm  int
	CurrentIndex int
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) get(index int) Entry {
	if index < -1 || index >= l.len() {
		panic(errors.New(fmt.Sprintf("error log index, total len: %d, get index: %d", l.len(), index)))
	}
	if index == -1 {
		return zeroEntry
	}
	return l.Entries[index]
}

func (l *Log) getLastLog() Entry {
	if len(l.Entries) == 0 {
		return zeroEntry
	}
	return l.Entries[len(l.Entries) - 1]
}
func (l *Log) getFirstLog() Entry {
	if len(l.Entries) == 0 {
		return zeroEntry
	}
	return l.Entries[0]
}
// index是要插入的位置，保留 0-index-1
func (l *Log) append(index int, entries Log)  {
	l.Entries = append(l.Entries[:index], entries.Entries...)
}

func (l *Log) appendEntries(args *AppendEntriesArgs) bool {
	matchIndex := l.len()-1
	for matchIndex >= 0 &&
		l.get(matchIndex).CurrentIndex != args.PrevLogIndex {
		matchIndex -= 1
	}
	// 不存在term匹配的记录，返回false
	if matchIndex < 0 ||
		l.get(matchIndex).CurrentTerm != args.PrevLogTerm {
		return false
	}

	// matchIndex之后的都要被删除
	l.append(matchIndex+1, args.Entries)
	return true
}