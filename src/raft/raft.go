package raft

import (
	"6.824/src/labrpc"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// server角色状态
const (
	LEADER          = "leader"
	FOLLOWER        = "follower"
	CANDIDATE       = "candidate"
	ElectionMinLimit = 200
	ElectionWeight = 150
	ElectionTimeout = (ElectionWeight + ElectionMinLimit) * time.Millisecond // 1000Millsecond = 1秒, 超时的最低时限
)

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastRPCTime time.Time
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
	votedFor *int
	logs Log

	// 所有server的可变状态
	commitIndex int // 已知的 最大的 已经被提交的 log index, 从0开始单增。 0意味着还没有提交，因为log从1开始编号
	lastApplied int // 最大的在状态机中的log index, 从0开始增加

	// leader的可变状态
	nextIndex []int //长度为len(peer)的数组，保存对每个server，下一个要发送的log index, 初始化为leader最新的log index + 1
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

func (rf *Raft) resetElectionTimeOut()  {
	rf.electionTime = time.Duration(ElectionMinLimit + rand.Intn(ElectionWeight)) * time.Millisecond
}
func (rf *Raft) beforeElection() int {
	// Paper Candidate 5.2
	rf.mu.Lock()
	rf.lastRPCTime = time.Now()
	rf.resetElectionTimeOut()
	rf.role = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = &rf.me
	rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) startElection(currentTerm int)  {
	//DPrintf("服务[%d]超时，正在发起第[%d]轮选举, 当前任期[%d]\n", rf.me, currentTerm,rf.currentTerm)
	if rf.currentTerm < currentTerm || rf.currentTerm == 0{
		panic(errors.New("启动选举失败\n"))
	}
	// 当前选举的Term
	// 1. 有server回复比较慢，但是新leader已经出来(role==follower), 可以结束本次选举
	// 2. 一段时间未产生， 新选举已经启动，也可以放弃
	numPeers := rf.getPeerNum()
	numRequest := 1
	numVote := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.role != CANDIDATE ||
			rf.currentTerm > currentTerm { // 新leader已经产生且收到了心跳
			break
		}
		go func(server int) {
			ok, reply := rf.SendRequestVoteRPC(server, currentTerm)
			if ok == true {
				rf.mu.Lock()
				numRequest = numRequest + 1
				rf.mu.Unlock()
			}
			if ok == true && reply.VotedMe == true {
				// 追求性能可以不加锁， 没选举出来在选举一轮即可
				rf.mu.Lock()
				numVote += 1
				rf.mu.Unlock()
			}
		}(peer)
	}

	// 1. 新leader还未产生 2. 还没有足够的返回 3. 选票还不够成为leader 4. 没有产生新的选举
	for rf.role == CANDIDATE &&
		numRequest < numPeers &&
		numVote <= numPeers / 2 &&
		rf.currentTerm == currentTerm &&
		time.Now().Before(rf.lastRPCTime.Add(rf.electionTime)){
		//rf.lastRPCTime = time.Now()
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}

	// 1. 产生了新的选举 => 对应for循环的4
	// 2. 新leader产生 => 1
	if rf.currentTerm > currentTerm || rf.role != CANDIDATE || time.Now().After(rf.lastRPCTime.Add(rf.electionTime)){
		//DPrintf("[%d] 竞选[%d]失败, 当前任期[%d], 当前身份[%s], 选票[%d]",rf.me,currentTerm,rf.currentTerm,rf.role,numVote)
		return
	}
	// => 3
	if numVote <= numPeers / 2 {
		//DPrintf("[%d] 收到RPC回应: %d, 收到选票: %d", rf.me, numRequest, numVote)
		return
	}

	// 可能在上一步中断，然后出现了新选举
	if numVote > numPeers / 2 &&
		rf.currentTerm == currentTerm &&
		rf.role == CANDIDATE {
		//DPrintf("服务[%d]成为[%d]的leader,peers[%d], votes[%d]\n",rf.me,rf.currentTerm,numPeers,numVote)
		rf.transferToLeader()
	}
}

func (rf *Raft) transferToCandidate(currentTerm int)  {
	// 在接收RPC时才会调用，发起选举不会调用
	rf.mu.Lock()
	rf.role = CANDIDATE
	// 有新的选举则置为null
	if rf.currentTerm != currentTerm {
		rf.votedFor = nil
	}
	rf.currentTerm = currentTerm
	rf.mu.Unlock()
	return
}

func (rf *Raft) transferToLeader()  {
	rf.mu.Lock()
	rf.role = LEADER
	rf.sendHeartBeatToAll()
	rf.votedFor = nil
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.nextIndex[peer] = rf.logs.getLastLog().CurrentIndex + 1
		rf.matchIndex[peer] = 0
	}
	rf.mu.Unlock()
	go rf.listenCommit()
	go rf.checkCommitIndex()
}

func (rf *Raft) transferToFollower(currentTerm int)  {
	//DPrintf("服务[%d]成为任期[%d]的follower",rf.me,currentTerm)
	rf.mu.Lock()
	if currentTerm < rf.currentTerm {
		panic("transferToFollower error")
	}
	rf.role = FOLLOWER
	rf.currentTerm = currentTerm
	rf.votedFor = nil
	rf.mu.Unlock()
}

func (rf *Raft) checkCommitIndex()  {
	currentTerm := rf.currentTerm
	for rf.role == LEADER && rf.currentTerm == currentTerm {
		nextCommitIndex := rf.logs.getLastLog().CurrentIndex
		totalNum := len(rf.peers)
		for rf.role == LEADER && rf.logs.find(nextCommitIndex).CurrentTerm == currentTerm && rf.commitIndex < nextCommitIndex{
			num := 0
			for peer := range rf.peers {
				//DPrintf("服务[%d]的match index:%d", peer, rf.matchIndex[peer])
				if rf.matchIndex[peer] >= nextCommitIndex {
					num += 1
				}
			}
			//DPrintf("日志:[%d] 已经被%d个服务复制", nextCommitIndex, num)
			if num > totalNum / 2 {
				rf.commitIndex = max(rf.commitIndex, nextCommitIndex)
				break
			} else {
				nextCommitIndex -= 1
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeatToAll()  {
	//rf.lastRPCTime = time.Now()
	// 检查commitIndex是否需要更新

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.role != LEADER {
			break
		}
		go rf.sendHeartBeat(peer)
	}
}

func (rf *Raft) sendHeartBeat(server int)  {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.logs.getLastLog().CurrentIndex,
		PrevLogTerm:  rf.logs.getLastLog().CurrentTerm,
		Entries:      Log{},
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	// 不再是LEADER
	if ok == true && reply.CurrentTerm > rf.currentTerm {
		rf.transferToFollower(reply.CurrentTerm)
		return
	}

	if ok == true && reply.Success == true {
		rf.nextIndex[server] = args.PrevLogIndex + 1
		rf.matchIndex[server] = args.PrevLogIndex
	}
}

func (rf *Raft) voteToServer(args *RequestVoteArgs, reply *RequestVoteReply) {
	// RPC中
	if args.CurrentTerm < rf.currentTerm {
		reply.VotedMe = false
		reply.CurrentTerm = rf.currentTerm
		return
	}

	if args.CurrentTerm > rf.currentTerm {
		rf.transferToFollower(args.CurrentTerm)
	}
 	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	reply.VotedMe = false
	reply.CurrentTerm = rf.currentTerm
	rf.mu.Lock()
	latestLogTerm := rf.logs.getLastLog().CurrentTerm
	latestLogIndex := rf.logs.getLastLog().CurrentIndex
	rf.mu.Unlock()
	// 只有当candidate的日志更新时才会投票
	// 更新指: 1. term 更大 2. 相等的term和更大的index
	if  args.LastLogTerm > latestLogTerm ||
		(args.LastLogTerm == latestLogTerm && args.LastLogIndex >= latestLogIndex)  {
		if rf.votedFor == nil || *rf.votedFor == args.CandidateIndex {
			rf.lastRPCTime = time.Now()
			rf.votedFor = &args.CandidateIndex
			reply.VotedMe = true
		}
		//DPrintf("服务[%d] 在Term: %d 投票给 %d 成功", rf.me, args.CurrentTerm, args.CandidateIndex)
	}
	//DPrintf("服务[%d]在[%d]轮投票给[%d]结果[%v]",rf.me,args.CurrentTerm,args.CandidateIndex,reply.VotedMe)
	return
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// current Term: 候选人term assert CurrentTerm >= rf.CurrentTerm
	// CandidateIndex: 候选人id
	CurrentTerm    int
	CandidateIndex int
	LastLogIndex   int
	LastLogTerm    int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// CurrentTerm: 对方的term，万一对方更新了，我可以更新
	// VotedMe: 是否投给我了
	CurrentTerm int
	VotedMe     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("服务[%d]收到投票请求，候选人[%d], 选举term[%d], 当前身份[%s], 当前选票[%v], 当前term[%d]\n",rf.me,args.CurrentTerm,args.CandidateIndex,rf.role,rf.votedFor,rf.currentTerm)
	// Your code here (2A, 2B).
	// 过期请求
	if args.CurrentTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VotedMe = false
		return
	}
	rf.lastRPCTime = time.Now()
	// 转换到Candidate, 当前Term状态为 args.CurrentTerm

	rf.voteToServer(args, reply)
	return
}

//
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
func (rf *Raft) SendRequestVoteRPC(server int, term int) (bool, *RequestVoteReply) {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		CurrentTerm:    term,
		CandidateIndex: rf.me,
		LastLogIndex: rf.logs.getLastLog().CurrentIndex,
		LastLogTerm: rf.logs.getLastLog().CurrentTerm,
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	if rf.role != CANDIDATE {
		reply.VotedMe = false
		reply.CurrentTerm = rf.currentTerm
		return false, reply
	}
	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)
	return ok, reply
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("sendRequestVote\n")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) tryCommit(server int, currentTerm int) {
	end := false
	for end == false && rf.role == LEADER && rf.currentTerm == currentTerm {
		rf.mu.Lock()
		nextCommitIndex := rf.nextIndex[server]
		prevCommitIndex := nextCommitIndex - 1
		prevLog := rf.logs.find(prevCommitIndex)

		args := &AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.CurrentIndex,
			PrevLogTerm:  prevLog.CurrentTerm,
			Entries:      rf.logs.getFrom(nextCommitIndex),
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()

		ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)

		if ok == false {
			// rpc失败
			time.Sleep(100 * time.Millisecond)
		} else {
			if reply.CurrentTerm > rf.currentTerm {
				rf.transferToLeader()
				return
			} else if reply.Success == true {
				rf.mu.Lock()
				matchIndex := args.Entries.getLastLog().CurrentIndex
				rf.nextIndex[server] = max(rf.nextIndex[server],  matchIndex + 1)
				rf.matchIndex[server] = max(rf.matchIndex[server], matchIndex)
				rf.mu.Unlock()
				end = true
			} else if reply.Success == false {
				// 日志不同步
				rf.mu.Lock()
				rf.nextIndex[server] = min(rf.nextIndex[server], prevCommitIndex)
				// TODO 当前matchIndex是未知状态
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
			} else {
				panic("tryCommit")
			}
		}
	}
}

func (rf *Raft) listenCommit()  {
	for rf.killed() == false && rf.role == LEADER {
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.role != LEADER {
				return
			}
			latestLogIndex := rf.logs.getLastLog().CurrentIndex
			if rf.matchIndex[server] == latestLogIndex {
				continue
			}
			go rf.tryCommit(server, rf.currentTerm)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) applyCommandToMachine()  {
	//DPrintf("leader[%d]开始从[%d]写入状态机, commit index:%d", rf.me, rf.lastApplied, rf.commitIndex)

	for rf.lastApplied <= rf.commitIndex && rf.role == LEADER{
		rf.mu.Lock()
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs.find(rf.lastApplied).Command,
			CommandIndex:  rf.lastApplied,
			SnapshotValid: false, // todo
			Snapshot:      nil, // todo
			SnapshotTerm:  0, // todo
			SnapshotIndex: 0, // todo
		}
		rf.mu.Unlock()
		select {
		case rf.stateMachine <- msg:
			rf.mu.Lock()
			rf.lastApplied += 1
			rf.mu.Unlock()
			//DPrintf("服务[%d]成功将日志[%d]写入状态机,apply:%d, commit:%d", rf.me, msg.CommandIndex, rf.lastApplied, rf.commitIndex)
		}
	}
	return
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	if rf.role != LEADER {
		return -1, -1, false
	}

	rf.mu.Lock()
	prevLog := rf.logs.getLastLog()
	newLog := Log{Entries: []Entry{{
		Command:      command,
		CurrentTerm:  rf.currentTerm,
		CurrentIndex: prevLog.CurrentIndex + 1,
	}}}
	rf.logs.appendLastLog(newLog)
	index = rf.logs.getLastLog().CurrentIndex
	term = rf.logs.getLastLog().CurrentTerm
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	//DPrintf("leader[%d]开始共识，index:[%d], 当前日志:\n%s", rf.me, index, rf.logs.String())
	rf.mu.Unlock()

	if rf.role != LEADER {
		isLeader = false
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) listenStateMachine()  {
	// todo
	for rf.killed() == false{
		if rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			index := rf.lastApplied + 1
			entry := rf.logs.find(index)
			rf.mu.Unlock()
			if entry.CurrentIndex == 0 {
				//DPrintf("服务[%d] 尝试将zeroEmpty加入状态机, index: %d, term: %d", rf.me, index, rf.currentTerm)
				continue
			}
			rf.mu.Lock()



			msg := ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.CurrentIndex,
				SnapshotValid: false, //todo
				Snapshot:      nil, //todo
				SnapshotTerm:  0, //todo
				SnapshotIndex: 0, //todo
			}
			rf.stateMachine <- msg
			rf.lastApplied += 1
			//DPrintf("服务[%d]将日志[%s]加入状态机,apply:%d, commitIndex: %d, logs:\n%s", rf.me, entry, rf.lastApplied,entry.CurrentIndex, rf.logs)
			rf.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	fmt.Printf("服务[%d] 已启动\n", rf.me)
	time.Sleep(time.Duration(rand.Intn(100)))
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//DPrintf("服务[%d]身份[%s],term[%d]\n",rf.me,rf.role,rf.currentTerm)
		if rf.role == LEADER {
			// 发心跳
			//DPrintf("服务[%d]是%d期的leader，开始发送心跳\n", rf.me, rf.currentTerm)
			go rf.sendHeartBeatToAll() //不一定立刻返回
		}
		// 超时，开始选举
		if rf.role != LEADER && time.Now().After(rf.lastRPCTime.Add(rf.electionTime)) {
			currentTerm := rf.beforeElection() //这个要阻塞
			//DPrintf("服务[%d]启动选举[%d]",rf.me,currentTerm)
			//不能阻塞, 选举中应该可以接受RPC请求
			go rf.startElection(currentTerm)
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(200)))
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
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
	rf.logs = Log{}
	// volatile
	rf.commitIndex = 0
	rf.lastApplied = 0
	// leader's volatile
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.listenStateMachine()

	return rf
}
