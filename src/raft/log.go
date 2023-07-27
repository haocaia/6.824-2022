package raft

type Entry struct {
	command interface{}
	term    int
	index   int
}
type Log struct {
	entry []Entry
}

func EmptyLog() Log {
	return Log{
		entry: []Entry{},
	}
}

func (log *Log) GetLastEntry() Entry {
	length := len(log.entry)
	if length == 0 {
		panic("GetLastEntry: length == 0")
	}
	return log.entry[length-1]
}

func (log *Log) GetLastEntryIndex() int {
	length := len(log.entry)
	if length == 0 {
		panic("GetLastEntryIndex: length == 0")
	}
	return log.entry[length-1].index
}

func (log *Log) GetLastEntryTerm() int {
	length := len(log.entry)
	if length == 0 {
		panic("GetLastEntryTerm: length == 0")
	}
	return log.entry[length-1].term
}

func (log *Log) Before(index, term int) bool {
	e := log.GetLastEntry()
	currentIndex := e.index
	currentTerm := e.term
	if currentIndex < index {
		return true
	}
	if currentIndex == index && currentTerm < term {
		return true
	}
	return false
}
