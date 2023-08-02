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
	rpcValid := RpcCompare(args.Term, raftTerm)
	if rpcValid == -1 {
		reply.Term = raftTerm
		reply.VoteGranted = false
		return
	}

	if rpcValid == 1 {
		rf.mu.Lock
		rf.TransToFollower(args.Term)
		rf.mu.Unlock
	}

	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm

	rf.mu.Lock()
	raftTerm = rf.term
	if rf.log.BeforeOf(lastLogIndex, lastLogTerm) &&
		(rf.vote == -1 || rf.vote == args.Id) {
		rf.TransToCandidate(args.Id, args.Term)

		reply.Term = raftTerm
		reply.VoteGranted = true
	}
	rf.mu.Unlock()

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ----------------------AppendEntries-------------------------
type AppendEntriesArgs struct {
}

type AppendEntriesReply struct {
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func RpcCompare(RpcTerm, RaftTerm int) int {
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
