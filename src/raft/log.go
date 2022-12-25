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
	CurrentTerm:  0,
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

func createLog(index, term int) Log {
	return Log{Entries: []Entry{{
		Command:      -1,
		CurrentTerm:  index,
		CurrentIndex: term,
	}}}
}

func (l *Log) len() int {
	return len(l.Entries)
}

// 查找currentIndex，并返回这个下标
func (l *Log) indexAt(currentIndex int) int {
	if currentIndex > l.getLastLog().CurrentIndex {
		//return Entry{
		//	Command:      -1,
		//	CurrentTerm:  -2,
		//	CurrentIndex: -2,
		//}
		panic(any(fmt.Sprintf("indexAt index: %d out of range: %d", currentIndex, l.getLastLog().CurrentIndex)))
	}
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
	if currentIndex > l.getLastLog().CurrentIndex {
		return Entry{
			Command:      -1,
			CurrentTerm:  -2,
			CurrentIndex: -2,
		}
		//panic(any(fmt.Sprintf("find index: %d out of range: %d", currentIndex, l.getLastLog().CurrentIndex)))
	}

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

// 给定日志currentLogIndex, 返回该日志以及之后的日志
func (l *Log) getFrom(currentIndex int) Log {
	if currentIndex > l.getLastLog().CurrentIndex {
		return Log{Entries: []Entry{}}
		//panic(any(fmt.Sprintf("getFrom currentIndex: %d out of range: %d", currentIndex, l.getLastLog().CurrentIndex)))
	}
	i := l.len() - 1
	for ; i >= 0 && l.Entries[i].CurrentIndex >= currentIndex; i -= 1 {
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
	if index < 0 || index > l.len() {
		panic(any(fmt.Sprintf("appendLog currentIndex: %d out of range: %d", index, l.len())))
	}
	l.Entries = append(l.Entries[:index], entries.Entries...)
}

func (l *Log) appendEntries(index int, entries []Entry) {
	if index < 0 || index > l.len() {
		panic(any(fmt.Sprintf("appendEntries currentIndex: %d out of range: %d", index, l.len())))
	}
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

// 是否存在， 数组下标， currentTerm, currentIndex
// 如果不存在匹配term和index的日志， 返回false， 如果index > len(log), 日志index返回len(log), 否则返回Zero entry
// 如果存在，还会返回在数组中的下标和对应的term和日志index
func (l *Log) checkPrevLogExist(term, index int) (bool, int, int, int) {
	if l.len() < index {
		return false, -1, -1, l.len()
	}
	matchIndex := l.len() - 1
	for matchIndex >= 0 &&
		l.get(matchIndex).CurrentIndex != index {
		matchIndex -= 1
	}

	if matchIndex != -1 && l.Entries[matchIndex].CurrentTerm != term {
		conflictTerm := l.Entries[matchIndex].CurrentTerm
		conflictIndex := l.Entries[matchIndex].CurrentIndex

		i := matchIndex
		for i >= 0 {
			if l.Entries[i].CurrentTerm == conflictTerm {
				conflictIndex = l.Entries[i].CurrentIndex
			}
			i -= 1
		}
		return false, i, conflictTerm, conflictIndex
	}

	// 不存在term匹配的记录，返回false
	if matchIndex < 0 && index != 0 {
		//DPrintf("不存在term匹配的记录， prevLogIndex[%d], prevLogTerm:[%d]", index, term)
		panic("check prev log error")
		return false, -1, -1, l.len()
	}
	if matchIndex == -1 && index == 0 {
		return true, matchIndex, zeroEntry.CurrentTerm, zeroEntry.CurrentIndex
	}
	return true, matchIndex, l.Entries[matchIndex].CurrentTerm, l.Entries[matchIndex].CurrentIndex
}

// 找到term==conflictTerm的最后一条日志， 返回下一条日志对应index
// 如果不存在，返回conflictIndex
func (l *Log) findNextIndex(conflictTerm int, conflictIndex int) int {
	// 这个是数组下标，不是log index, 需要做一次转换
	matchIndex := l.len() - 1
	for matchIndex >= 0 &&
		l.Entries[matchIndex].CurrentTerm > conflictTerm {
		matchIndex -= 1
	}

	// 不存在conflictTerm
	if matchIndex == -1 || l.Entries[matchIndex].CurrentTerm != conflictTerm {
		return conflictIndex
	}

	return l.Entries[matchIndex].CurrentIndex + 1
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
