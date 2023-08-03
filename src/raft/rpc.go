package raft

/*
	1. 注意参数名大写
*/

// ----------------------RequestVote-------------------------
type RequestVoteArgs struct {
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raftTerm := rf.term
	rpcValid := TermCompare(args.Term, raftTerm)
	if rpcValid == -1 {
		reply.Term = raftTerm
		reply.VoteGranted = false
		return
	}

	if rpcValid == 1 {
		rf.mu.Lock()
		rf.TransToFollower(args.Term)
		rf.mu.Unlock()
	}

	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm

	rf.mu.Lock()
	raftTerm = rf.term
	if rf.log.BeforeOrEqualOf(lastLogIndex, lastLogTerm) &&
		(rf.vote == -1 || rf.vote == args.Id) {
		rf.TransToCandidate(args.Id, args.Term)
		DPrintln("[%d] 在Term: [%d] 中投票给 [%d]", rf.me, args.Term, args.Id)
		reply.Term = raftTerm
		reply.VoteGranted = true
		rf.receiveRPC = true
	}
	rf.mu.Unlock()

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ----------------------AppendEntries-------------------------
type AppendEntriesArgs struct {
	Term         int
	Id           int
	PrevLogIndex int
	PrevLogTerm  int
	Log          Log
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := rf.term
	if isOutDate(args.Term, term) {
		reply.Success = false
		reply.Term = term
		return
	}
	rf.receiveRPC = true
	DPrintln("[%d] 收到 Term:[%d]的RPC, 当前Term: [%d]", rf.me, args.Term, rf.term)
	if isOutDate(term, args.Term) {
		rf.mu.Lock()
		rf.TransToFollower(args.Term)
		rf.mu.Unlock()
	}

	if !rf.log.exist(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.term
		return
	}

	rf.mu.Lock()
	rf.log.merge(args.Log)
	rf.mu.Unlock()

	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.CommitIndex, rf.log.GetLastEntryIndex())
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	term := args.Term
	if isOutDate(term, rf.term) {
		return false
	}
	if isOutDate(rf.term, reply.Term) {
		rf.mu.Lock()
		rf.TransToFollower(reply.Term)
		rf.mu.Unlock()
		return false
	}

	// TODO: 加速回溯
	// 如果因为日志不匹配失败才需要回溯
	if ok && reply.Success == false {
		rf.nextIndex[server] -= 1
	}

	return ok
}

func TermCompare(RpcTerm, RaftTerm int) int {
	// -1 rpc过期
	// 0 相等
	// 1 raft落后
	if RpcTerm < RaftTerm {
		return -1
	}
	if RpcTerm == RaftTerm {
		return 0
	}
	return 1
}

func isHeart(args *AppendEntriesArgs) bool {
	return args.Log.empty()
}
