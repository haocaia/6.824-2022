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

func createLog(term, index int, command interface{}) Log {
	return Log{Entries: []Entry{{
		Command:      command,
		CurrentTerm:  term,
		CurrentIndex: index,
	}}}
}

func (l *Log) len() int {
	return len(l.Entries)
}

// 查找currentIndex，并返回这个下标
func (l *Log) indexAt(currentIndex int) int {
	index := l.len() - 1
	// 找到第一个 index = currentIndex的位置, 找不到返回-1
	for ; index >= 0 &&
		l.Entries[index].CurrentIndex != currentIndex; index -= 1 {
		//do nothing
	}
	return index
}

// 查找currentIndex，并返回这个entry
func (l *Log) find(currentIndex int) Entry {
	index := l.len() - 1
	for ; index >= 0 &&
		l.Entries[index].CurrentIndex != currentIndex; index -= 1 {

	}
	if index < 0 {
		return zeroEntry
	}
	return l.Entries[index]
}

// 返回下标对应的log，如果下标是-1，返回zeroEntry
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
	for ; i >= 0 && l.Entries[i].CurrentIndex >= index; i -= 1 {
	}
	log := Log{Entries: l.Entries[i+1:]}
	return log
}

func (l *Log) getLastLog() Entry {
	if len(l.Entries) == 0 {
		return zeroEntry
	}
	return l.Entries[len(l.Entries)-1]
}
func (l *Log) getFirstLog() Entry {
	if len(l.Entries) == 0 {
		return zeroEntry
	}
	return l.Entries[0]
}

// index是要插入的位置，保留 0-index-1
func (l *Log) appendLog(index int, entries Log) {
	l.Entries = append(l.Entries[:index], entries.Entries...)
}

func (l *Log) appendEntries(index int, entries []Entry) {
	l.Entries = append(l.Entries[:index], entries...)
}

func (l *Log) appendLastLog(entries Log) {
	if entries.getFirstLog().CurrentIndex != l.getLastLog().CurrentIndex+1 {
		DPrintf("leader log不是单调增加1")
		return
	}
	l.Entries = append(l.Entries[:], entries.Entries...)
	return
}

func (l *Log) appendLastEntries(entries []Entry) {
	l.Entries = append(l.Entries[:], entries...)
}

// 如果不存在匹配term和index的日志， 返回false
// 如果存在，还会返回在数组中的下标
func (l *Log) checkPrevLogExist(term, index int) (bool, int) {
	matchIndex := l.len() - 1
	for matchIndex >= 0 &&
		l.get(matchIndex).CurrentIndex != index {
		matchIndex -= 1
	}
	// 不存在term匹配的记录，返回false
	if (matchIndex < 0 && index != 0) ||
		(matchIndex >= 0 && l.get(matchIndex).CurrentTerm != term) {
		//DPrintf("不存在term匹配的记录， prevLogIndex[%d], prevLogTerm:[%d]", index, term)
		return false, -1
	}
	return true, matchIndex
}

func (l *Log) findLatestMatchIndex(prevLogTerm int, prevLogIndex int) int {
	// 这个是数组下标，不是log index, 需要做一次转换
	matchIndex := l.len() - 1
	for matchIndex >= 0 &&
		(l.Entries[matchIndex].CurrentTerm != prevLogTerm || l.Entries[matchIndex].CurrentIndex != prevLogIndex) {
		matchIndex -= 1
	}

	if matchIndex == -1 {
		return zeroEntry.CurrentIndex
	}

	return l.Entries[matchIndex].CurrentIndex
}

func (e Entry) String() string {
	return fmt.Sprintf("{command: %v, term: %d index: %d}", e.Command, e.CurrentTerm, e.CurrentIndex)
}

func (l Log) String() string {
	var s string
	for i := 0; i < l.len(); i += 1 {
		s += fmt.Sprintf("日志[%d]: %s\n", l.get(i).CurrentIndex, l.get(i))
	}
	return s
}
