package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) IsElectionTimeOut() bool {
	return time.Since(rf.startElectionTime) > rf.electionTimeOut
}

// ChangeToxxx all ChangeToXXX need to lock before using and unlock after using
func (rf *Raft) ChangeToCandidate(term uint32) {
	fmt.Println(fmt.Sprintf("%d become candidate term %d", rf.me, rf.term+1))
	rf.state = CandidateState
	go rf.StartVote(term)
}

func (rf *Raft) ResetElection() {
	rf.startElectionTime = time.Now()
	rf.electionTimeOut = time.Duration(rand.Intn(200)+300) * time.Millisecond
}

func (rf *Raft) ChangeToFollower(term uint32, voteFor int, mod int) {
	if term < rf.term {
		return
	}
	fmt.Println(fmt.Sprintf("%d become follower term %d, mod %d", rf.me, term, mod))
	rf.state = FollowerState
	rf.term = term
	rf.voteFor = voteFor
	rf.ResetElection()
}

func (rf *Raft) ChangeToLeader() {
	fmt.Println(fmt.Sprintf("%d become leader term %d", rf.me, rf.term))
	rf.state = LeaderState
	rf.BrocastHeartBeat()
}

func (rf *Raft) StartVote(term uint32) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Because we use go func we need to check if rf change state or term before starting vote
	// we don't need to use this vote feature, if state change to follower or leader or term change
	if !rf.CheckState(CandidateState, term) {
		return
	}

	rf.voteNums.Store(1)
	rf.voteFor = rf.me
	rf.term += 1
	lastReply := RequestVoteReply{Term: rf.term, VoteGranted: false}

	arg := RequestVoteArgs{Term: rf.term, CandidateId: rf.me, LastLogTerm: 0, LastLogIndex: 0}
	wg := sync.WaitGroup{}

	// when this channel have val means this func can return
	done := make(chan VoteState, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		done <- VoteTimeOut
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			success := rf.sendRequestVote(x, &arg, &reply)
			if !success {
				fmt.Println(fmt.Sprintf("%d request rpc false, send to %d", rf.me, x))
				return
			}

			if reply.VoteGranted == false {
				// need to change state to follower
				lastReply = reply
				done <- NeedToBecomeFollower
				return
			} else {
				rf.voteNums.Add(1)
				if rf.voteNums.Load() > uint32(len(rf.peers)/2) {
					done <- VoteForLeader
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		done <- NotEnough
	}()

	result := <-done
	fmt.Println(fmt.Sprintf("%d result %d", rf.me, result))

	if result == VoteForLeader {
		rf.ChangeToLeader()
	} else {
		rf.ChangeToFollower(lastReply.Term, -1, 1)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	if rf.term > args.Term {
		fmt.Println(fmt.Sprintf("%d receive vote to %d request in term %d, not vote", rf.me, args.CandidateId, args.Term))
		return
	}

	if args.Term > rf.term {
		rf.ChangeToFollower(args.Term, -1, 2)
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && true {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		fmt.Println(fmt.Sprintf("%d receive vote to %d request in term %d, vote", rf.me, args.CandidateId, args.Term))
	}

	return
}

//func (rf *Raft) ChangeTerm(term uint32) {
//	if term > rf.term {
//		rf.state = FollowerState
//		rf.term = term
//		rf.voteFor = -1
//	}
//}

func (rf *Raft) RequestAppendEntriesRpc(args *AppendEntriesArgs, reply *AppendEntriesReplys) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println(fmt.Sprintf("%d receive appendentrices", rf.me))
	reply.Success = false

	if args.Term < rf.term {
		reply.Term = rf.term
		return
	}

	reply.Success = true

	if args.Term == rf.term {
		rf.ResetElection()
	} else {
		rf.ChangeToFollower(args.Term, -1, 3)
	}

	return
}

func (rf *Raft) CheckState(state RaftState, term uint32) bool {
	return rf.state == state && rf.term == term
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRpc(server int, args *AppendEntriesArgs, reply *AppendEntriesReplys) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntriesRpc", args, reply)
	return ok
}

func (rf *Raft) BrocastHeartBeat() {
	arg := AppendEntriesArgs{Term: rf.term, LeaderId: rf.me, PreLogIndex: 0, Entries: make([]interface{}, 0), LeaderCommit: 0}
	reply := AppendEntriesReplys{}

	for i := range rf.peers {
		if rf.me != i {
			go func(x int) {
				fmt.Println(fmt.Sprintf("%d send append entries rpc to %d", rf.me, x))
				rf.sendAppendEntriesRpc(x, &arg, &reply)

				// if reply.term > this term return to follower
				if reply.Term > rf.term {
					rf.ChangeToFollower(reply.Term, -1, 4)
					return
				}
			}(i)
		}
	}
}

func (rf *Raft) HeartBeatTicker() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)

		if rf.state == LeaderState {
			rf.mu.Lock()

			fmt.Println(fmt.Sprintf("now %d is leader term %d", rf.me, rf.term))
			rf.BrocastHeartBeat()

			rf.mu.Unlock()
		}
	}
}
