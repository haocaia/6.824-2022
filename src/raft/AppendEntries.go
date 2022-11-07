package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

//接受RPC
func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	//DPrintf("服务[%d]收到append请求，leader[%d], 日志长度:[%d] 心跳任期[%d] 当前任期[%d]\n",rf.me,args.LeaderId,args.Entries.len(),args.Term, rf.currentTerm)
	// 收到了一个无效的RPC
	if args.Term < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastRPCTime = time.Now()
	// 产生了新的Leader
	if args.Term > rf.currentTerm {
		rf.transferToFollower(args.Term)
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	}


	// 一个心跳，
	if args.Entries.len() == 0 {
		reply.Success = true
		reply.CurrentTerm = rf.currentTerm
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.logs.getLastLog().CurrentIndex)
		}
		return
	}


	//DPrintf("服务[%d]尝试添加日志[%d]后的内容", rf.me, args.PrevLogIndex)
	ok := rf.logs.appendEntries(args)

	reply.Success = ok
	reply.CurrentTerm = rf.currentTerm
	//DPrintf("服务[%d]收到Append包，leader的commitIndex[%d], commitIndex[%d]",rf.me,args.LeaderCommit, rf.logs.getLastLog().CurrentIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.getLastLog().CurrentIndex)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}