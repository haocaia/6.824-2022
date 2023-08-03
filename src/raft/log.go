package raft

import "fmt"

type Entry struct {
	Command interface{}
	Term  int
	Index int
}
type Log struct {
	Entry []Entry
}

func EmptyEntry() Entry {
	return Entry{
		Index: 0,
		Term: -1,
	}
}

func EmptyLog() Log {
	return Log{
		Entry: []Entry{EmptyEntry()},
	}
}

func (log *Log) GetLastEntry() Entry {
	length := len(log.Entry)
	if length == 0 {
		return Entry{
			Index: 0,
			Term:  -1,
		}
	}
	return log.Entry[length-1]
}

func (log *Log) GetLastEntryIndex() int {
	length := len(log.Entry)
	if length == 0 {
		return 0
	}
	return log.Entry[length-1].Index
}

func (log *Log) GetLastEntryTerm() int {
	length := len(log.Entry)
	if length == 0 {
		return -1
	}
	return log.Entry[length-1].Term
}

func (log *Log) BeforeOrEqualOf(index, term int) bool {
	e := log.GetLastEntry()
	currentIndex := e.Index
	currentTerm := e.Term
	if currentIndex < index {
		return true
	}
	if currentIndex == index && currentTerm == term {
		return true
	}
	return false
}

func (log *Log) empty() bool {
	return len(log.Entry) == 0
}

func (log *Log) size() int {
	return len(log.Entry)
}

// 查找logIndex的日志在切片中的下标i
func (log *Log) index(logIndex int) int {
	// TODO: 日志压缩
	return logIndex - 1
}

func (log *Log) get(i int) Entry {
	if i == -1 {
		return EmptyEntry()
	}
	if log.empty() || i >= log.size() {
		err := fmt.Sprintf("get out of bound, i = %d", i)
		panic(err)
	}
	return log.Entry[i]
}

func (log *Log) exist(index, term int) bool {
	if log.GetLastEntryIndex() < index {
		return false
	}
	i := log.index(index)
	return term == log.get(i).Term
}

func (log *Log) find(index, term int) int {
	if log.GetLastEntryIndex() < index {
		return -1
	}
	i := log.index(index)
	return i
}

func (log *Log) merge(newLog Log) {
	if newLog.empty() {
		return
	}
	startIndex := log.find(newLog.get(0).Index, newLog.get(0).Term)
	if startIndex == -1 {
		startIndex = log.size()
	}

	i := 0
	for startIndex < log.size() && i < newLog.size() &&
		log.get(startIndex).equal(newLog.get(i)) {
		startIndex += 1
		i += 1
	}

	result := append([]Entry{}, log.Entry[:startIndex]...)
	result = append(result, newLog.Entry[i:]...)
	log.Entry = result
}

func (e Entry) equal(v Entry) bool {
	return e.Index == v.Index && e.Term == v.Term
}

