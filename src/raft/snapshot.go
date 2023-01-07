package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshotRPC(server int, currentTerm int) (bool, int) {
	DPrintf("sendInstallSnapshotRPC")
	if rf.role != LEADER || rf.currentTerm != currentTerm || rf.logs.getLastIncludedIndex() <= 0 {
		//DPrintf("服务[%d]的sendInstallSnapshotRPC fail: [%s %d %d]", rf.me, rf.role, currentTerm, rf.logs.getLastIncludedIndex())
		return false, -1
	}
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:              currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.logs.getLastIncludedIndex(),
		LastIncludedTerm:  rf.logs.getLastIncludedTerm(),
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	if rf.role != LEADER || rf.currentTerm != currentTerm {
		return false, -1
	}

	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)

	if rf.role != LEADER || rf.currentTerm != currentTerm || ok == false {
		//DPrintf("leader[%d]给服务[%d]的InstallSnapshotRPC返回false, role: %s, term: %d, ok: %v, reply term:[%d]", rf.me, server, rf.role, rf.currentTerm, ok, reply.Term)
		return false, -1
	}
	//DPrintf("服务[%d] sendInstallSnapshotRPC success", server)
	if reply.Term > rf.currentTerm {
		rf.transferToFollower(reply.Term)
	}
	//rf.mu.Lock()
	if rf.role == LEADER && rf.currentTerm == currentTerm {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
	}
	//rf.mu.Unlock()
	//DPrintf("服务[%d]success, nextIndex:[%d], matchIndex:[%d]", server, rf.nextIndex[server], rf.matchIndex[server])
	return true, args.LastIncludedIndex
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//DPrintf("服务[%d]InstallSnapshotRPC启动", rf.me)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.transferToFollower(args.Term)
	}
	//if args.LastIncludedIndex <= rf.logs.getLastIncludedIndex() {
	//	return
	//}
	//rf.mu.Lock()
	//DPrintf("服务[%d]InstallSnapshotRPC更新snapshot, 当前日志\n%s", rf.me, rf.logs)
	ok := rf.logs.updateSnapshot(args.LastIncludedTerm, args.LastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	//DPrintf("服务[%d]InstallSnapshotRPC更新snapshot结果 %v,当前日志\n%s", rf.me, ok, rf.logs)
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	if ok == false {
		//DPrintf("服务[%d]更新snapshot[%d, %d]失败", rf.me, args.LastIncludedTerm, args.LastIncludedIndex)
		//rf.mu.Unlock()
		return
	}

	//DPrintf("服务[%d]从leader[%d]接收快照[%d, %d]成功，当前日志\n%s", rf.me, args.LeaderId, args.LastIncludedTerm, args.LastIncludedIndex, rf.logs)
	msg := ApplyMsg{
		CommandValid:  true,
		Command:       "apply snapshot",
		CommandIndex:  -1,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	//rf.mu.Unlock()
	//DPrintf("apply快照")
	rf.stateMachine <- msg
	//DPrintf("快照apply成功")

}
