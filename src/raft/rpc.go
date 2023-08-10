package raft

import (
	"fmt"
	"time"
)

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
		DPrintln("[%d] 在Term: [%d] 中投票给 [%d], 候选人日志: index = [%d] term = [%d], 我方日志: index = [%d] term = [%d]", rf.me, args.Term, args.Id, lastLogIndex, lastLogTerm, rf.log.GetLastEntryIndex(), rf.log.GetLastEntryTerm())
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
	Conflict bool
	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := rf.term
	heart := rf.log.empty()
	if isOutDate(args.Term, term) {
		reply.Success = false
		reply.Term = term
		DPrintln("[%d] 接收过期的AppendEntries", rf.me)
		return
	}
	rf.receiveRPC = true
	//DPrintln("[%d] 收到 Term:[%d]的RPC, 当前Term: [%d]", rf.me, args.Term, rf.term)
	if isOutDate(term, args.Term) {
		rf.mu.Lock()
		rf.TransToFollower(args.Term)
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	lastLogIndex := rf.log.GetLastEntryIndex()
	exist := rf.log.exist(args.PrevLogIndex, args.PrevLogTerm)
	rf.mu.Unlock()

	if exist != 1{
		rf.mu.Lock()
		if !heart {
			DPrintln("[%d] prev日志不存在. prevLogIndex=[%d], prevLogTerm=[%d], log=\n[%v]", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log)
		}
		reply.Success = false
		reply.Term = rf.term
		reply.Conflict = true
		if exist == 0 {
			reply.ConflictIndex = lastLogIndex
			reply.ConflictTerm = -1
		} else {
			conflictTerm := rf.log.GetEntryWithLogIndex(lastLogIndex).Term
			if conflictTerm == -1 {
				err := fmt.Sprintf("[%d] conflictTerm = -1, log latest log index = [%d], rpc lastLogIndex = [%d]", rf.me, rf.log.GetLastEntryIndex(), lastLogIndex)
				panic(err)
			}
			conflictIndex := rf.log.FindFirstIndexWithTerm(conflictTerm)
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = conflictIndex
		}

		DPrintln("[%d] 接收日志失败, PrevLogIndex = [%d], PrevLogTerm = [%d], 当前日志 = \n %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log)
		rf.mu.Unlock()
		return
	}
	if !heart {
		DPrintln("[%d] 接收日志成功, prevLog index = [%d], term = [%d], 当前日志 = \n%v\n新日志 = \n%v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log, args.Log)
	}
	rf.mu.Lock()
	//DPrintln("[%d] 的最新日志=[%d], 正在合并新日志[%d]", rf.me, rf.log.GetLastEntryIndex(), args.Log.GetLastEntryIndex())
	rf.log.merge(args.Log)
	if !heart {
		DPrintln("[%d] 日志合并成功，最新日志=[%d]", rf.me, rf.log.GetLastEntryIndex())
	}
	rf.mu.Unlock()

	if args.CommitIndex > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		latestLogIndex := args.PrevLogIndex
		if !args.Log.empty() {
			latestLogIndex = args.Log.GetLastEntryIndex()
		}
		rf.mu.Lock()
		rf.commitIndex = min(args.CommitIndex, latestLogIndex)
		rf.mu.Unlock()
		DPrintln("[%d] 更新commit index:[%d] -> [%d]", rf.me, oldCommitIndex, rf.commitIndex)
	}

	reply.Success = true
	reply.Conflict = false
	reply.Term = term
}

func (rf *Raft) sendAppendEntries(server int, isHeart bool) {
	term := rf.term
	if isOutDate(term, rf.term) {
		return
	}
	if !isLeader(rf.role) {
		return
	}
	for {
		rf.mu.Lock()
		nextIndex := rf.nextIndex[server]
		prevLogIndex := rf.log.GetPrevLogIndex(nextIndex)
		prevLogTerm := rf.log.GetPrevLogTerm(nextIndex)
		commitIndex := rf.commitIndex
		//DPrintln("[%d] 开始发rpc, nextIndex = [%d] prevLog index=[%d], term=[%d]", rf.me, nextIndex, prevLogIndex, prevLogTerm)
		if !isHeart && rf.log.GetLastEntryIndex() + 1 < nextIndex {
			err := fmt.Sprintf("last log index + 1 < next index. [%d] < [%d]", rf.log.GetLastEntryIndex() + 1, nextIndex)
			panic(err)
		}
		var args *AppendEntriesArgs
		if isHeart {
			args = &AppendEntriesArgs{
				Term:         term,
				Id:           rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Log:          EmptyLog(),
				CommitIndex:  commitIndex,
			}
		} else {
			args = &AppendEntriesArgs{
				Term:         term,
				Id:           rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Log:          rf.log.GetLogFromLogIndex(nextIndex),
				CommitIndex:  commitIndex,
			}
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{
			Term:    term,
			Success: false,
			Conflict: false,
		}
		if args.Log.size() > 0 {
			DPrintln("[%d] 尝试给[%d]发送日志from [%d] to [%d] , prevLogIndex = [%d], prevLogTerm[%d]", rf.me, server, args.Log.Entry[0].Index,  args.Log.GetLastEntryIndex(),args.PrevLogIndex, args.PrevLogTerm)
		} else {
		//	DPrintln("[%d] 尝试给[%d]发送心跳, nextIndex = [%d], prevLogIndex = [%d], prevLogTerm[%d]", rf.me, server, rf.nextIndex[server], args.PrevLogIndex, args.PrevLogTerm)
		}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if isOutDate(term, rf.term) {
			return
		}
		if !isLeader(rf.role) {
			return
		}
		if isOutDate(rf.term, reply.Term) {
			rf.mu.Lock()
			rf.TransToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		// TODO: 加速回溯
		// 如果因为日志不匹配失败才需要回溯
		if !ok {
			time.Sleep(time.Duration(DEFAULT_SLEEP_MS) * time.Millisecond)
		} else {
			if reply.Success {
				// update next index and match index

				rf.nextIndex[server] = max(rf.nextIndex[server], args.Log.GetLastEntryIndex() + 1)
				rf.matchIndex[server] = max(rf.matchIndex[server], args.Log.GetLastEntryIndex())
				if !isHeart {
					DPrintln("[%d]接收日志到 [%d] 成功， 更新nextIndex = [%d], matchIndex = [%d]", server, args.Log.GetLastEntryIndex(), rf.nextIndex[server], rf.matchIndex[server])
				}
				return
			} else if reply.Success == false {
				if reply.Conflict{
					if reply.ConflictTerm == -1 {
						// rf.nextIndex[server] = max(rf.nextIndex[server] - 1, 1)
						rf.nextIndex[server] = max(reply.ConflictIndex, 1)
					} else {
						rf.mu.Lock()
						index := rf.log.FindFirstIndexWithTerm(reply.ConflictTerm)
						if index == -1 {
							// 不存在term
							rf.nextIndex[server] = max(reply.ConflictIndex, 1)
						} else {
							index = rf.log.FindLastIndexWithTerm(reply.ConflictTerm)
							if index < 1 {
								err := fmt.Sprintf("[%d] FindLastIndexWithTerm = -1", rf.me)
								panic(err)
							}
							rf.nextIndex[server] = max(index + 1, 1)
						}
						rf.mu.Unlock()
					}

				}
				//DPrintln("[%d]接收日志失败, nextIndex = [%d]", server, rf.nextIndex[server])
				time.Sleep(time.Duration(DEFAULT_SLEEP_MS) * time.Millisecond)
			}
		}
		if isHeart {
			return
		}
	}
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

