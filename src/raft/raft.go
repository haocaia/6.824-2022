package raft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// server角色状态
const (
	LEADER           = "leader"
	FOLLOWER         = "follower"
	CANDIDATE        = "candidate"
	ElectionMinLimit = 400
	ElectionWeight   = 150                                                    // 100 + rand(ElectionWeight)
	ElectionTimeout  = (ElectionWeight + ElectionMinLimit) * time.Millisecond // 1000Millsecond = 1秒, 超时的最低时限
)

// as each Raft peer becomes aware that successive log Entries are
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastRPCTime  time.Time
	electionTime time.Duration
	stateMachine chan ApplyMsg
	// role
	// LEADER:
	// 定期发送心跳包（不包含log的AppendRPC）
	// FOLLOWER:
	// 保持FOLLOWER,当他收到有效的RPC从一个LEADER或者CANDIDATE
	// 选举超时时间内没有收到RPC请求，发起一次选举
	// CANDIDATE, 保持本状态直到以下3件事之一发生:
	// 1. 我赢得了选举，role == LEADER
	// 2. 其他人赢得了选举
	// 3. 一段时间没人赢得选举
	role string

	// 所有server的持久化状态
	// currentTerm: 最新的term id， 从0开始增加
	// votedFor: 投票给某个server， 还没投票为null
	// logs: 保存的所有log，无论是否提交，从1开始编号
	currentTerm int
	votedFor    *int
	logs        Log

	// 所有server的可变状态
	commitIndex int // 已知的 最大的 已经被提交的 log index, 从0开始单增。 0意味着还没有提交，因为log从1开始编号
	lastApplied int // 最大的在状态机中的log index, 从0开始增加

	// leader的可变状态
	nextIndex  []int //长度为len(peer)的数组，保存对每个server，下一个要发送的log index, 初始化为leader最新的log index + 1
	matchIndex []int //保存已知的每个server的已经被成功复制的index index, 初始化为0
}

func (rf *Raft) getPeerNum() int {
	return len(rf.peers)
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.role == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	vote := -1
	if rf.votedFor != nil {
		vote = *rf.votedFor
	}
	e.Encode(vote)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var vote int
	var log Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&vote) != nil ||
		d.Decode(&log) != nil {
		panic(any("decode error"))
	} else {
		rf.currentTerm = currentTerm
		if vote == -1 {
			rf.votedFor = nil
		} else {
			rf.votedFor = &vote
		}
		rf.logs = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) resetElectionTimeOut() {
	rf.electionTime = time.Duration(ElectionMinLimit+rand.Intn(ElectionWeight)) * time.Millisecond
}

func (rf *Raft) startElection(currentTerm int) {
	//DPrintf("服务[%d]超时，正在发起第[%d]轮选举, 当前任期[%d]\n", rf.me, currentTerm, rf.currentTerm)
	if rf.currentTerm < currentTerm || rf.currentTerm <= 0 {
		panic(errors.New("启动选举失败\n"))
	}

	numPeers := rf.getPeerNum()
	numRequest := 1
	numVote := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.role != CANDIDATE ||
			rf.currentTerm > currentTerm {
			break
		}
		go func(server int) {
			ok, reply := rf.SendRequestVoteRPC(server, currentTerm)
			if ok == false {
				return
			}
			if reply.CurrentTerm > rf.currentTerm {
				rf.transferToFollower(reply.CurrentTerm)
				return
			}
			if rf.currentTerm > currentTerm {
				return
			}
			if ok == true {
				rf.mu.Lock()
				numRequest = numRequest + 1
				if reply.VotedMe == true {
					numVote += 1
				}
				rf.mu.Unlock()
			}
		}(peer)
	}

	// 1. 新leader还未产生 2. 还没有足够的返回 3. 选票还不够成为leader 4. 没有产生新的选举
	for rf.role == CANDIDATE &&
		numRequest < numPeers &&
		numVote <= numPeers/2 &&
		rf.currentTerm == currentTerm &&
		time.Now().Before(rf.lastRPCTime.Add(rf.electionTime)) {
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}

	if rf.currentTerm > currentTerm || rf.role != CANDIDATE || time.Now().After(rf.lastRPCTime.Add(rf.electionTime)) || numVote <= numPeers/2 {
		return
	}

	if rf.role == CANDIDATE && numVote > numPeers/2 && rf.currentTerm == currentTerm {
		rf.transferToLeader()
	}
}

func (rf *Raft) transferToCandidate() int {
	// Paper Candidate 5.2
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = &rf.me
	rf.lastRPCTime = time.Now()
	rf.resetElectionTimeOut()
	rf.role = CANDIDATE
	currentTerm := rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	return currentTerm
}

func (rf *Raft) transferToLeader() {
	rf.mu.Lock()
	DPrintf("服务[%d] 成为[%d]term的leader", rf.me, rf.currentTerm)
	rf.role = LEADER
	go rf.sendHeartBeatToAll()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.nextIndex[peer] = rf.logs.getLastLog().CurrentIndex + 1
		rf.matchIndex[peer] = 0
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	//rf.persist()
	go rf.listenCommit(currentTerm)
	go rf.checkCommitIndex(currentTerm)
}

func (rf *Raft) transferToFollower(currentTerm int) {
	//DPrintf("服务[%d]成为任期[%d]的follower", rf.me, currentTerm)
	rf.mu.Lock()
	rf.lastRPCTime = time.Now()
	rf.resetElectionTimeOut()
	if currentTerm <= rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.role = FOLLOWER
	rf.votedFor = nil
	//if currentTerm > rf.currentTerm {
	//	rf.votedFor = nil
	//}
	rf.currentTerm = currentTerm
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) checkCommitIndex(currentTerm int) {
	for rf.killed() == false && rf.role == LEADER && rf.currentTerm == currentTerm {
		//DPrintf("LEADER[%d]检查commitIndex", rf.me)
		nextCommitIndex := rf.commitIndex + 1
		//DPrintf("LEADER[%d]检查nextCommitIndex[%d]的位置[%d]", rf.me, nextCommitIndex, rf.logs.indexAt(nextCommitIndex))
		if rf.logs.indexAt(nextCommitIndex) < 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// 1. Term<=0: nextCommitIndex不存在
		// 2. Term不一致: 不能提交非当前term日志
		for rf.role == LEADER && rf.currentTerm == currentTerm &&
			rf.logs.find(nextCommitIndex).CurrentTerm != currentTerm &&
			rf.logs.find(nextCommitIndex).CurrentTerm > 0 {
			nextCommitIndex += 1
		}
		//DPrintf("LEADER[%d]检查完nextCommitIndex[%d], 位置[%d]", rf.me, nextCommitIndex, rf.logs.indexAt(nextCommitIndex))
		if rf.role != LEADER || rf.currentTerm != currentTerm ||
			rf.logs.indexAt(nextCommitIndex) < 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		totalNum := len(rf.peers)
		for rf.role == LEADER &&
			currentTerm == rf.currentTerm &&
			rf.logs.find(nextCommitIndex).CurrentTerm == currentTerm &&
			rf.commitIndex < nextCommitIndex {
			num := 1
			voted := []int{rf.me}
			for peer := range rf.peers {
				if peer == rf.me {
					continue
				}
				//DPrintf("leader检查服务[%d]的matchIndex:[%d], 期望:[%d]", peer, rf.matchIndex[peer], nextCommitIndex)
				if rf.matchIndex[peer] >= nextCommitIndex {
					voted = append(voted, peer)
					num += 1
				}
			}
			//DPrintf("日志:[%d] 已经被%d个服务复制", nextCommitIndex, num)
			if rf.role == LEADER && currentTerm == rf.currentTerm && num > totalNum/2 {
				DPrintf("leader[%d %d]更新commitIndex:[%d] -> [%d], 已经复制的节点:%v", rf.currentTerm, rf.me, rf.commitIndex, max(rf.commitIndex, nextCommitIndex), voted)
				rf.commitIndex = max(rf.commitIndex, nextCommitIndex)
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeatToAll() {
	currentTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.role != LEADER || currentTerm != rf.currentTerm {
			break
		}
		//DPrintf("LEADER [%d] 给服务[%d]发心跳,term:[%d]", rf.me, peer, currentTerm)
		go rf.sendHeartBeat(peer, currentTerm)
	}
}

func (rf *Raft) sendHeartBeat(server int, currentTerm int) {

	//rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.logs.getLastLog().CurrentIndex,
		PrevLogTerm:  rf.logs.getLastLog().CurrentTerm,
		Entries:      createLog(-1, -1, "心跳包"),
		LeaderCommit: rf.commitIndex,
	}
	//rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	if ok == false {
		return
	}

	if reply.CurrentTerm > rf.currentTerm {
		rf.transferToFollower(reply.CurrentTerm)
		return
	}

	if reply.Success == true {
		rf.mu.Lock()
		if rf.currentTerm == currentTerm && rf.role == LEADER {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex)
			rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) voteToServer(args *RequestVoteArgs, reply *RequestVoteReply) {
	// RPC中
	//DPrintf("VoteToServer")
	if args.CurrentTerm < rf.currentTerm {
		reply.VotedMe = false
		reply.CurrentTerm = rf.currentTerm
		return
	}

	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	rf.mu.Lock()
	reply.VotedMe = false
	reply.CurrentTerm = args.CurrentTerm

	latestLogTerm := rf.logs.getLastLog().CurrentTerm
	latestLogIndex := rf.logs.getLastLog().CurrentIndex
	rf.mu.Unlock()
	// 只有当candidate的日志更新时才会投票
	// 更新指: 1. term 更大 2. 相等的term和更大的index
	DPrintf("投票信息: Leader[%d]最新日志:[%d, %d], Leader term: %d,我的最新日志:[%d, %d], 服务[%d]的term和投票情况: [%d, %v]", args.CandidateIndex, args.LastLogTerm, args.LastLogIndex, args.CurrentTerm, rf.logs.getLastLog().CurrentTerm, rf.logs.getLastLog().CurrentIndex, rf.me, rf.currentTerm, rf.votedFor)
	if args.LastLogTerm > latestLogTerm ||
		(args.LastLogTerm == latestLogTerm && args.LastLogIndex >= latestLogIndex) {
		rf.mu.Lock()
		if rf.votedFor == nil {
			rf.lastRPCTime = time.Now()
			rf.votedFor = &args.CandidateIndex
			//DPrintf("服务[%d]在term[%d]的选举中投票给[%d]", rf.me, args.CurrentTerm, args.CandidateIndex)
			reply.VotedMe = true
			if rf.role == FOLLOWER {
				rf.role = CANDIDATE
			}
			rf.persist()
		}
		//DPrintf("服务[%d] 在Term: [%d] 的竞选中投票给 [%d]", rf.me, args.CurrentTerm, args.CandidateIndex)
		rf.mu.Unlock()
	}
	reply.CurrentTerm = rf.currentTerm
	if args.CurrentTerm != rf.currentTerm {
		reply.VotedMe = false
	}
	//DPrintf("服务[%d]在[%d]轮投票给[%d]结果[%v]", rf.me, args.CurrentTerm, args.CandidateIndex, reply.VotedMe)
	return
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// current Term: 候选人term assert CurrentTerm >= rf.CurrentTerm
	// CandidateIndex: 候选人id
	CurrentTerm    int
	CandidateIndex int
	LastLogIndex   int
	LastLogTerm    int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// CurrentTerm: 对方的term，万一对方更新了，我可以更新
	// VotedMe: 是否投给我了
	CurrentTerm int
	VotedMe     bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("服务[%d]收到投票请求，候选人[%d], 选举term[%d], 当前身份[%s], 当前选票[%v], 当前term[%d]\n",rf.me,args.CurrentTerm,args.CandidateIndex,rf.role,rf.votedFor,rf.currentTerm)
	// Your code here (2A, 2B).
	// 过期请求
	if args.CurrentTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VotedMe = false
		return
	}
	if args.CurrentTerm > rf.currentTerm {
		//DPrintf("服务[%d]收到更大的Term:[%d],变为follower", rf.me, args.CurrentTerm)
		rf.transferToFollower(args.CurrentTerm)
	}
	rf.voteToServer(args, reply)
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// 调用本方法可以给server发一个RequestVoteRPC
// server: 希望从server中获得票的选举人id
// term: 选举的term, 如果term < rf.currentTerm,则应该放弃本轮选举
func (rf *Raft) SendRequestVoteRPC(server int, currentTerm int) (bool, *RequestVoteReply) {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		CurrentTerm:    currentTerm,
		CandidateIndex: rf.me,
		LastLogIndex:   rf.logs.getLastLog().CurrentIndex,
		LastLogTerm:    rf.logs.getLastLog().CurrentTerm,
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	if rf.role != CANDIDATE || rf.currentTerm > currentTerm {
		reply.VotedMe = false
		reply.CurrentTerm = rf.currentTerm
		return false, reply
	}
	if rf.currentTerm < currentTerm {
		panic("SendRequestVoteRPC term error")
	}
	//DPrintf("服务[%d]发送选举", rf.me)
	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)

	if rf.role != CANDIDATE || rf.currentTerm > currentTerm {
		reply.VotedMe = false
		reply.CurrentTerm = max(reply.CurrentTerm, rf.currentTerm)
		return false, reply
	}
	return ok, reply
}

func (rf *Raft) tryCommit(currentTerm int, server int) {
	// 只表示循环是否结束，不代表commit成功
	//DPrintf("tryCommit[%d]", server)
	rf.mu.Lock()
	nextCommitIndex := rf.nextIndex[server]
	if nextCommitIndex > rf.logs.getLastLog().CurrentIndex {
		//DPrintf("服务[%d]的nextCommitIndex[%d], leader最大日志:%d", server, nextCommitIndex, rf.logs.getLastLog().CurrentIndex)
		rf.mu.Unlock()
		return
	}
	//DPrintf("leader=[%d], server=[%d]的nextCommitIndex:[%d]", rf.me, server, rf.nextIndex[server])
	prevLog := rf.logs.find(nextCommitIndex - 1)
	// 不存在,1:已经被快照
	if prevLog.CurrentTerm < 0 {
		// TODO 发送snapshot
		//rf.mu.Unlock()
		//return
		prevLog = rf.logs.Entries[0]
	}
	allLog := rf.logs.getFrom(nextCommitIndex)
	allLog.Entries = append([]Entry{createEntry(rf.logs.getLastIncludedTerm(), rf.logs.getLastIncludedIndex(), "snapshot")}, allLog.Entries[:]...)
	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLog.CurrentIndex,
		PrevLogTerm:  prevLog.CurrentTerm,
		Entries:      allLog,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	//DPrintf("leader[%d]给[%d]发送[%d]之后的log:\n%s", rf.me, server, prevLog.CurrentIndex, allLog.String())
	//DPrintf("服务[%d]发送Commit Append RPC包", rf.me)
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	//DPrintf("服务[%d]给[%d]发送[%d]的结果[%v], [%v]", rf.me, server, prevLog.CurrentIndex, ok, reply.Success)
	if ok == false || currentTerm < rf.currentTerm || rf.role != LEADER {
		if reply.CurrentTerm > rf.currentTerm {
			rf.transferToFollower(reply.CurrentTerm)
		}
		return
	}
	//DPrintf("服务[%d]给[%d]发送[%d]的结果[%v], [%v]", rf.me, server, prevLog.CurrentIndex, ok, reply.Success)
	if reply.CurrentTerm > rf.currentTerm {
		//DPrintf("服务[%d]在commit时收到更大的term: %d", rf.me, reply.CurrentTerm)
		rf.transferToFollower(reply.CurrentTerm)
		return
	} else if reply.Success == true {
		if args.Entries.empty() == false {
			rf.mu.Lock()
			if currentTerm == rf.currentTerm && rf.role == LEADER {
				matchIndex := args.Entries.getLastLog().CurrentIndex
				rf.matchIndex[server] = max(rf.matchIndex[server], matchIndex)
				rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
			}
			rf.mu.Unlock()
		}
	} else if reply.Success == false {
		rf.mu.Lock()
		if currentTerm == rf.currentTerm && rf.role == LEADER {
			conflictTerm := reply.ConflictTerm
			conflictIndex := reply.ConflictIndex
			rf.nextIndex[server] = rf.logs.findNextIndex(conflictTerm, conflictIndex)
			//DPrintf("服务[%d]复制日志失败， conflict log:[%d %d], nextIndex: [%d]", server, conflictTerm, conflictIndex, rf.nextIndex[server])
		}
		rf.mu.Unlock()
	} else {
		panic("try commit error")
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	/**
	1. command 加到leader的末尾
	2. 并发 发送给follower复制
	3. 安全复制后，leader送到state machine, 然后返回结果
	4. (3)中如果有follower慢了/挂了，无限循环尝试复制
	*/

	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	if rf.role != LEADER {
		isLeader = false
		return index, term, isLeader
	}
	//DPrintf("Term[%d]leader[%d]收到command[%v],开始共识", rf.currentTerm, rf.me, command)

	//1. 加锁保证index每次+1
	//DPrintf("LEADER[%d]等锁", rf.me)
	rf.mu.Lock()
	//DPrintf("Term[%d]leader[%d]收到command[%v],开始共识", rf.currentTerm, rf.me, command)
	prevLog := rf.logs.getLastLog()
	term = rf.currentTerm
	index = prevLog.CurrentIndex + 1
	if rf.role == LEADER && rf.currentTerm == term {
		rf.logs.appendLastLog(term, index, command)
		rf.persist()
		isLeader = true //必须返回true
		//DPrintf("leader服务[%d]开启新的共识[%d], leader日志:\n%s", rf.me, index, rf.logs)
		// 更新leader自己的nextIndex和matchIndex
		rf.matchIndex[rf.me] = max(rf.matchIndex[rf.me], index)
		rf.nextIndex[rf.me] = max(rf.nextIndex[rf.me], rf.matchIndex[rf.me]+1)
	}
	rf.mu.Unlock()
	//DPrintf("结束共识")
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

// 不能单线程，会卡死
func (rf *Raft) listenCommit(currentTerm int) {
	for rf.role == LEADER && rf.currentTerm == currentTerm {
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.nextIndex[server] > rf.logs.getLastLog().CurrentIndex {
				continue
			}
			if rf.role != LEADER || rf.currentTerm != currentTerm {
				return
			}

			go rf.tryCommit(currentTerm, server)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) listenStateMachine() {
	// 单线程，需要保证apply的顺序
	for rf.killed() == false {
		if rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			index := rf.lastApplied + 1
			entry := rf.logs.find(index)
			DPrintf("服务[%d] commitIndex:[%d], 正在apply:[%d, %d, %v]", rf.me, rf.commitIndex, entry.CurrentTerm, index, entry.Command)
			if entry.CurrentIndex < 0 {
				rf.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				continue
			}
			rf.lastApplied += 1

			msg := ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.CurrentIndex,
				SnapshotValid: false, //todo
				Snapshot:      nil,   //todo
				SnapshotTerm:  0,     //todo
				SnapshotIndex: 0,     //todo
			}
			rf.mu.Unlock()

			rf.stateMachine <- msg

			//DPrintf("服务[%d]将日志[%s]加入状态机,apply:%d, commitIndex: %d, logs:\n%s", rf.me, entry, rf.lastApplied, entry.CurrentIndex, rf.logs)
			//DPrintf("服务[%d]将日志[%d, %d]加入状态机,apply:%d, commitIndex: %d", rf.me, entry.CurrentTerm, entry.CurrentIndex, rf.lastApplied, entry.CurrentIndex)
			//rf.mu.Unlock()
		} else {
			time.Sleep(50 * time.Millisecond)
		}

	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	fmt.Printf("服务[%d] 已启动\n", rf.me)
	time.Sleep(time.Duration(rand.Intn(100)))
	for rf.killed() == false {
		if rf.role == LEADER {
			//DPrintf("leader[%d] in term: [%d] ticker", rf.me, rf.currentTerm)
			go rf.sendHeartBeatToAll()
		}
		// 超时选举
		if rf.role != LEADER && time.Now().After(rf.lastRPCTime.Add(rf.electionTime)) {
			//DPrintf("服务[%d]开始选举，当前时间%v", rf.me, rf.lastRPCTime.String())
			currentTerm := rf.transferToCandidate()
			go rf.startElection(currentTerm)
			//time.Sleep(rf.electionTime - time.Duration(300)*time.Millisecond) // 额外休眠，防止抢别人时间
			time.Sleep(150 * time.Millisecond)
			rf.lastRPCTime = time.Now()
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(300)))
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
	// Your initialization code here (2A, 2B, 2C).
	// rf init
	rf.role = FOLLOWER //没有任何选举
	rf.stateMachine = applyCh
	// persistent, 以下参数需要在readPersist中读取和保存
	rf.currentTerm = 0 // 初始为0，readPersist后会更改
	rf.votedFor = nil
	rf.logs = createLog(0, 0, "zero log")
	// volatile
	rf.commitIndex = 0
	rf.lastApplied = 0
	// leader's volatile
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.listenStateMachine()

	return rf
}
