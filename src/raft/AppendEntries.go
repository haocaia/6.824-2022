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

	ConflictTerm  int
	ConflictIndex int
}

// 接受RPC
func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
	}
	// 检查PrevLog是否存在
	exist, index, conflictTerm, conflictIndex := rf.logs.checkPrevLogExist(args.PrevLogTerm, args.PrevLogIndex)
	reply.CurrentTerm = args.Term // 只有args.Term能保证一致，rf.currentTerm可能增大
	reply.Success = exist
	if exist == false {
		reply.Success = false
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = conflictIndex
		return
	}

	rf.mu.Lock()
	//DPrintf("before:服务[%d]尝试添加日志[%d]后的内容", rf.me, args.PrevLogIndex)
	// !! 当且仅当follower的日志与append中 **存在** 的日志冲突时才截断，
	if args.Entries.len() > 0 {
		rf.logs.appendLog(index+1, args.Entries)
		//DPrintf("服务[%d]收到非心跳appendRPC, log:[%v], prevTerm: [%d], prevIndex=[%d], exist=[%v], index=[%d], 当前日志\n%s", rf.me, args.Entries, args.PrevLogTerm, args.PrevLogIndex, exist, index, rf.logs)
		//DPrintf("服务[%d]收到非心跳AppendRPC,当前日志\n%s", rf.me, rf.logs.String())
	}

	rf.mu.Unlock()

	if args.LeaderCommit > rf.commitIndex {
		rf.mu.Lock()
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, rf.logs.getLastLog().CurrentIndex)
		//DPrintf("服务[%d]更新commitIndex:[%d], leader commit: [%d], 当前日志:\n%s",rf.me,rf.commitIndex,args.LeaderCommit,rf.logs.String())
		rf.mu.Unlock()
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
