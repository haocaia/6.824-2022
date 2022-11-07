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
	Command      interface{}
	CurrentTerm  int
	CurrentIndex int
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) indexAt(currentIndex int) int {
	index := l.len()-1
	// 找到第一个 index = currentIndex的位置, 找不到返回-1
	for ;index >= 0 && l.Entries[index].CurrentIndex != currentIndex;index -= 1 {
		//do nothing
	}
	return index
}

func (l *Log) find(currentIndex int) Entry {
	index := l.len()-1
	for ;index >= 0 && l.Entries[index].CurrentIndex != currentIndex;index -= 1 {

	}
	if index < 0 {
		return zeroEntry
	}
	return l.Entries[index]
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

func (l *Log) getFrom(index int) Log {
	i := l.len() - 1
	for ;i >= 0 && l.Entries[i].CurrentIndex >= index;i-=1{}
	log := Log{Entries: l.Entries[i+1:]}
	return log
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

func (l *Log) appendLast(entries Log)  {
	l.Entries = append(l.Entries[:], entries.Entries...)
}

func (l *Log) appendEntries(args *AppendEntriesArgs) bool {
	matchIndex := l.len()-1
	for matchIndex >= 0 &&
		l.get(matchIndex).CurrentIndex != args.PrevLogIndex {
		matchIndex -= 1
	}
	// 不存在term匹配的记录，返回false
	if (matchIndex < 0 && args.PrevLogIndex != 0)||
		l.get(matchIndex).CurrentTerm != args.PrevLogTerm {
		DPrintf("不存在term匹配的记录， prevLogIndex[%d], prevLogTerm:[%d]", args.PrevLogIndex, args.PrevLogTerm)
		return false
	}

	// matchIndex之后的都要被删除
	l.append(matchIndex+1, args.Entries)
	return true
}