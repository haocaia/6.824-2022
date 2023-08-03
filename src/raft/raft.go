package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log Entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()
	applyCh chan ApplyMsg

	role string
	term int
	vote int
	log  Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	receiveRPC bool
	firstIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int = rf.term
	var isleader bool = isLeader(rf.role)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	leader := isLeader(rf.role)

	if !leader {
		return index, term, leader
	}

	rf.mu.Lock()
	term = rf.term
	index = rf.log.appendEntry(command, term)
	if rf.firstIndex == -1 {
		rf.firstIndex = index
	}
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

	DPrintln("[%d]新增一条log[%d]", rf.me, rf.log.GetLastEntryIndex())
	go rf.sendAppendEntriesToAll(term)

	return index, term, leader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if isLeader(rf.role) {
			go rf.sendHeartToAll(rf.term)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 这个方法中不允许直接执行函数, 统一创建协程处理!!!
func (rf *Raft) rpcListener() {
	for rf.killed() == false {
		// TODO: 超时时间设置为了至少1个心跳时间+随机的300ms, 如果超时可以稍微减少
		ms := 350 + (rand.Int63() % 300)
		rf.receiveRPC = false
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if rf.receiveRPC == false && !isLeader(rf.role){
			DPrintln("[%d]在term: [%d]超时, receiveRPC=[%v]", rf.me, rf.term, rf.receiveRPC)
			go rf.startElection()
		}
	}
}

func (rf *Raft) commitListener() {
	for rf.killed() == false {
		// TODO: 超时时间设置为了至少1个心跳时间+随机的300ms, 如果超时可以稍微减少
		ms := DEFAULT_SLEEP_MS + (rand.Int63() % 50)
		term := rf.term
		if isLeader(rf.role) {
			update := false
			index := max(rf.firstIndex, rf.commitIndex + 1)
			if rf.log.GetEntryWithLogIndex(index).Term == term {
				num := 0
				numNeed := (len(rf.peers) + 1) / 2
				for server := 0; server < len(rf.peers); server += 1 {
					if rf.matchIndex[server] >= index {
						num += 1
					}
				}
				rf.mu.Lock()
				if num >= numNeed && term == rf.term && isLeader(rf.role) && rf.log.GetEntryWithLogIndex(index).Term == term {
					DPrintln("[%d] 更新commit index到[%d]", rf.me, rf.commitIndex)
					rf.commitIndex = max(rf.commitIndex, index)
					update = true
				}
				rf.mu.Unlock()
			}
			if !update {
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		} else {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

	}
}

func (rf *Raft) applyListener() {
	for rf.killed() == false {
		ms := DEFAULT_SLEEP_MS + (rand.Int63() % 50)
		if rf.commitIndex > rf.lastApplied {
			success := false
			index := rf.lastApplied + 1
			if index <= rf.log.GetLastEntryIndex() {
				entry := rf.log.GetEntryWithLogIndex(index)
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       entry.Command,
					CommandIndex:  entry.Index,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				rf.applyCh <- msg
				DPrintln("[%d] apply msg: [%d]", rf.me, index)
				success = true
			}
			if !success {
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		} else {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.role = FOLLOWER
	rf.term = 0
	rf.vote = -1
	rf.log = EmptyLog()

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.receiveRPC = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.rpcListener()
	go rf.commitListener()
	go rf.applyListener()

	return rf
}

func (rf *Raft) sendAppendEntriesToAll(term int) {
	for server := 0; server < len(rf.peers); server += 1 {
		if server == rf.me {
			continue
		}
		if isOutDate(term, rf.term) {
			return
		}
		if !isLeader(rf.role) {
			return
		}

		rf.mu.Lock()
		nextIndex := rf.nextIndex[server]
		prevLogIndex := rf.log.GetPrevLogIndex(nextIndex)
		prevLogTerm := rf.log.GetPrevLogTerm(nextIndex)
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		if rf.log.GetLastEntryIndex() < nextIndex {
			err := fmt.Sprintf("last log index < next index. [%d] < [%d]", rf.log.GetLastEntryIndex(), nextIndex)
			panic(err)
		}

		args := &AppendEntriesArgs{
			Term:         term,
			Id:           rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Log:          rf.log.GetLogFromLogIndex(nextIndex),
			CommitIndex:  commitIndex,
		}
		reply := &AppendEntriesReply{
			Term:    term,
			Success: false,
		}
		if args.Log.empty() {
			panic("新日志的内容为空")
		}
		DPrintln("[%d] 尝试给[%d]发送日志from [%d]", rf.me, server, args.Log.Entry[0].Index)
		go rf.sendAppendEntries(server, args, reply)
	}
}

func (rf *Raft) sendHeartToAll(term int) {
	if isOutDate(term, rf.term) {
		return
	}
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         term,
		Id:           rf.me,
		PrevLogIndex: rf.log.GetLastEntryIndex(),
		PrevLogTerm:  rf.log.GetLastEntryTerm(),
		Log:          EmptyLog(),
		CommitIndex:  rf.commitIndex,
	}
	rf.mu.Unlock()

	for server := 0; server < len(rf.peers); server += 1 {
		if server == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		go rf.sendAppendEntries(server, args, reply)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.term = rf.term + 1
	rf.TransToCandidate(rf.me, rf.term)
	term := rf.term
	lastLogIndex := rf.log.GetLastEntryIndex()
	lastLogTerm := rf.log.GetLastEntryTerm()
	rf.mu.Unlock()

	voteMe := 1
	numWin := (len(rf.peers) + 1) / 2
	numReplied := 1

	args := &RequestVoteArgs{
		Term:         term,
		Id:           rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for server := 0; server < len(rf.peers); server += 1 {
		if server == rf.me {
			continue
		}
		// 如果启动选举时的term < rf.Term， 说明该请求已过期，不再处理
		if isOutDate(term, rf.term) {
			return
		}
		go func(server int) {
			reply := &RequestVoteReply{
				Term:        -1,
				VoteGranted: false,
			}
			rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			numReplied += 1
			rf.mu.Unlock()
			if rf.term < reply.Term {
				rf.mu.Lock()
				rf.TransToFollower(reply.Term)
				rf.mu.Unlock()
				return
			}
			if isOutDate(term, rf.term) {
				return
			}
			if reply.VoteGranted {
				voteMe += 1
			}
		}(server)
	}

	maxWaitMs := 360
	maxTime := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)
	for numReplied < len(rf.peers) && voteMe < numWin && !isOutDate(term, rf.term) && time.Now().Before(maxTime) {
		time.Sleep(time.Duration(DEFAULT_SLEEP_MS) * time.Millisecond)
	}
	DPrintln("[%d] 在Term: [%d] 中的投票结果:\n共计选票: [%d]\n获得选票: [%d]\n", rf.me, args.Term, numReplied, voteMe)
	if voteMe >= numWin && !isOutDate(term, rf.term) {
		DPrintln("[%d] 在Term: [%d] 中成为Leader", rf.me, term)
		rf.mu.Lock()
		rf.TransToLeader(term)
		rf.mu.Unlock()
	}

}

func (rf *Raft) TransToLeader(term int) {
	if rf.term > term {
		return
	}

	rf.role = LEADER
	rf.vote = -1
	rf.firstIndex = -1

	lastLogIndex := rf.log.GetLastEntryIndex()
	for server := 0; server < len(rf.peers); server += 1 {
		rf.nextIndex[server] = lastLogIndex + 1
		rf.matchIndex[server] = 0
	}
	DPrintln("[%d] 在Term: [%d] 中成为Leader, 开始发送心跳", rf.me, term)
	go rf.sendHeartToAll(term)
}

func (rf *Raft) TransToFollower(term int) {
	if rf.term >= term {
		return
	}

	rf.role = FOLLOWER
	rf.vote = -1
	rf.term = max(term, rf.term)
}

func (rf *Raft) TransToCandidate(id, term int) {
	if rf.term != term {
		return
	}
	rf.role = CANDIDATE
	rf.vote = id
	rf.receiveRPC = true
}

func isLeader(role string) bool {
	return role == LEADER
}
