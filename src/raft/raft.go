package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
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
// committed log entry.
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
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role string
	term int
	vote int
	log  Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	rpcCh chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
			go rf.sendHeartToAll()
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
		ms := 50 + (rand.Int63() % 300)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			// 开启选举
			go rf.startElection()
		case <-rf.rpcCh:
			// 接收到RPC, 可以重置选举时间
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

	rf.role = FOLLOWER
	rf.term = 0
	rf.vote = -1
	rf.log = EmptyLog()

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.rpcCh = make(chan int, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.rpcListener()

	return rf
}

func (rf *Raft) sendHeartToAll() {

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
		reply := &RequestVoteReply{
			Term:        -1,
			VoteGranted: false,
		}
		// 如果启动选举时的term < rf.term， 说明该请求已过期，不再处理
		if isOutDate(term, rf.term) {
			return
		}
		go func() {
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
		}()
	}

	maxWaitMs := 360
	maxTime := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)
	for numReplied < len(rf.peers) && voteMe < numWin && !isOutDate(term, rf.term) && time.Now().Before(maxTime) {
		time.Sleep(time.Duration(DEFAULT_SLEEP_MS) * time.Millisecond)
	}

	if voteMe >= numWin && !isOutDate(term, rf.term) {
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

	lastLogIndex := rf.log.GetLastEntryIndex()
	for server := 0; server < len(rf.peers); server += 1 {
		rf.nextIndex[server] = lastLogIndex + 1
		rf.matchIndex[server] = 0
	}

	go rf.sendHeartToAll()
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
	rf.rpcCh <- 1
}

func isLeader(role string) bool {
	return role == LEADER
}
