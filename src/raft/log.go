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

func createEntry(term, index int, command interface{}) Entry {
	return Entry{
		Command:      command,
		CurrentTerm:  term,
		CurrentIndex: index,
	}
}

func (l *Log) len() int {
	return len(l.Entries)
}

// 查找currentIndex日志的下标
// currentIndex应该单增，因此就在currentIndex - lastIncludeIndex这个下标
// 下标范围[lastIncludeIndex+1, lastIncludeIndex+len-1]
// 若存在，返回对应的下标。若不存在则返回-1
func (l *Log) indexAt(currentIndex int) int {
	index := currentIndex - l.getLastIncludedIndex()
	if index < 0 || index >= l.len() {
		return -1
	}
	if index == 0 && l.Entries[index].CurrentIndex != currentIndex {
		return -1
	}
	return index
}

// 查找currentIndex，并返回这个entry
// 只有当lastLogIndex >= currentIndex >= lastIncludedIndex才会返回。否则返回-1,-1,cmd
func (l *Log) find(currentIndex int) Entry {
	index := l.indexAt(currentIndex)
	if index < 0 {
		return createEntry(-1, -1, "currentIndex不存在")
	}
	oldEntry := l.Entries[index]
	newEntry := createEntry(oldEntry.CurrentTerm, oldEntry.CurrentIndex, oldEntry.Command)
	return newEntry
}

// 返回下标对应的log
// index: 日志对应下标，[1,len-1]
// 如果下标是-1，返回zeroEntry
func (l *Log) get(index int) Entry {
	if index < 0 || index >= l.len() {
		panic(errors.New(fmt.Sprintf("error log index, total len: %d, get index: %d", l.len(), index)))
	}
	oldEntry := l.Entries[index]
	newEntry := createEntry(oldEntry.CurrentTerm, oldEntry.CurrentIndex, oldEntry.Command)
	return newEntry
}

// 返回currentIndex(包含)及之后的日志。
// 若不存在currentIndex，返回空日志
func (l *Log) getFrom(currentIndex int) Log {
	index := l.indexAt(currentIndex)
	if index < 0 {
		return createLog(-1, -1, fmt.Sprintf("getFrom找不到日志%d", currentIndex))
	}

	log := Log{Entries: append([]Entry{}, l.Entries[index:]...)}
	return log
}

// 返回最后一个日志
// 若日志为空则报错
func (l *Log) getLastLog() Entry {
	if l.len() == 0 {
		panic("getLastLog len == 0")
	}

	oldEntry := l.Entries[l.len()-1]
	newEntry := createEntry(oldEntry.CurrentTerm, oldEntry.CurrentIndex, oldEntry.Command)
	return newEntry
}

// 返回最新一个日志
func (l *Log) getFirstLog() Entry {
	if len(l.Entries) <= 1 {
		panic("log len <= 1")
	}
	oldEntry := l.Entries[1]
	newEntry := createEntry(oldEntry.CurrentTerm, oldEntry.CurrentIndex, oldEntry.Command)
	return newEntry
}

// index是要插入的位置，插入位置>1
func (l *Log) appendLog(index int, entries Log) {
	if index <= 0 {
		panic("appendLog index <= 0")
	}
	l.Entries = append(l.Entries[:index], entries.Entries[1:]...)
}

func (l *Log) appendLastLog(term, index int, command interface{}) {
	lastLogIndex := l.Entries[l.len()-1].CurrentIndex
	if lastLogIndex+1 != index {
		panic("appendLastLog: index != lastIndex + 1")
	}
	l.Entries = append(l.Entries, createEntry(term, index, command))
	return
}

// 检查Leader的PrevLog在server日志中是否存在
// 1. prevLog超过server日志范围，返回 false, -1, len(log)
// 2. 存在prevLogIndex但是Term不匹配，conflictTerm = log[prevLogIndex].Term，然后找到Term==conflictTerm的第一个日志
// 如果存在，还会返回在数组中的下标和对应的term和日志index
func (l *Log) checkPrevLogExist(currentTerm, currentIndex int) (bool, int, int) {
	index := l.indexAt(currentIndex)
	if index < 0 {
		return false, -1, l.getLastLog().CurrentIndex
	}
	oldLog := make([]Entry, l.len())
	copy(oldLog, l.Entries)
	if oldLog[index].CurrentTerm != currentTerm {
		conflictTerm := oldLog[index].CurrentTerm
		firstIndex := index
		for firstIndex-1 > 0 && oldLog[firstIndex-1].CurrentTerm == conflictTerm {
			firstIndex -= 1
		}
		conflictIndex := oldLog[firstIndex].CurrentIndex
		return false, conflictTerm, conflictIndex
	}

	return true, currentTerm, currentIndex
}

// 找到term==conflictTerm的最后一条日志， 返回下一条日志对应index
// 如果不存在，返回conflictIndex
func (l *Log) findNextIndex(conflictTerm int, conflictIndex int) int {
	// 这个是数组下标，不是log index, 需要做一次转换
	oldLog := make([]Entry, l.len())
	copy(oldLog, l.Entries)
	matchIndex := len(oldLog) - 1
	for matchIndex >= 0 &&
		oldLog[matchIndex].CurrentTerm > conflictTerm {
		matchIndex -= 1
	}

	// 不存在conflictTerm
	if matchIndex == -1 || oldLog[matchIndex].CurrentTerm != conflictTerm {
		return conflictIndex
	}

	return oldLog[matchIndex].CurrentIndex + 1
}

func (l *Log) getLastIncludedIndex() int {
	return l.Entries[0].CurrentIndex
}

func (l *Log) getLastIncludedTerm() int {
	return l.Entries[0].CurrentTerm
}

func (l *Log) empty() bool {
	if l.len() == 0 {
		panic("empty error, log len == 0")
	}
	return l.len() <= 1
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
