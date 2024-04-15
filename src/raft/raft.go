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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	FollowerState RaftState = iota
	CandidateState
	LeaderState
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	term      uint32              // this node term
	voteNums  atomic.Uint32       // nums of votes revived
	state     RaftState           // node state
	leaderID  int                 // leader number in peers
	heartBeat bool                // receive heartbeat or not
	voteFore  int                 // votefor which id in this term
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = int(rf.term)
	isleader = rf.state == LeaderState

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
	// Your code here (3C).
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
	// Your code here (3C).
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
	// Your code here (3D).

}

// This struct will be send with appendentrics RPC.
// It used to send heartbeat, push force commit.
type AppendEntriesArgs struct {
	Term         uint32        // leader's term
	LeaderId     int           // leader's id for this system
	PreLogIndex  int           // term of preLogIndex entry
	Entries      []interface{} // log entries to store
	LeaderCommit int           // transaction which has be committed
}

type AppendEntriesReplys struct {
	Term    uint32 // currentTerm for this machine
	Success bool   // true if follower contained entry match preLogIndex and preLogTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint32 // candidate's term
	CandidateId  int    // this machine id
	LastLogIndex uint32 // index of candidate's last log entry
	LastLogTerm  uint32 // term of candidate's last entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint32 // currentTerm for candidate to update itself
	VoteGranted bool   // if last request was received or not
}

type VoteState int

const (
	VoteTimeOut VoteState = iota
	NotEnough
	NeedToBecomeFollower
	VoteForLeader
)

// ChangeToxxx all ChangeToXXX need to lock before using and unlock after using
func (rf *Raft) ChangeToCandidate() {
	fmt.Println(fmt.Sprintf("%d become candidate term %d", rf.me, rf.term+1))
	go rf.StartVote()
}

func (rf *Raft) ChangeToFollower(term uint32, voteFor int) {
	fmt.Println(fmt.Sprintf("%d become follower term %d", rf.me, rf.term))
	rf.state = FollowerState
	rf.ChangeTerm(term)
}

func (rf *Raft) ChangeToLeader() {
	fmt.Println(fmt.Sprintf("%d become leader term %d", rf.me, rf.term))
	rf.state = LeaderState
}

func (rf *Raft) ChangeTerm(term uint32) {
	if term > rf.term {
		rf.state = FollowerState
		rf.term = term
		rf.voteFore = -1
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	if rf.term > args.Term {
		fmt.Println(fmt.Sprintf("%d receive vote to %d request, not vote", rf.me, args.CandidateId))
		return
	}

	rf.ChangeTerm(args.Term)

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.voteFore == -1 || rf.voteFore == args.CandidateId) && true {
		reply.VoteGranted = true
		rf.voteFore = args.CandidateId
		fmt.Println(fmt.Sprintf("%d receive vote to %d request, vote", rf.me, args.CandidateId))
	}

	return
}

func (rf *Raft) RequestAppendEntriesRpc(args *AppendEntriesArgs, reply *AppendEntriesReplys) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println(fmt.Sprintf("%d receive appendentrices", rf.me))

	rf.ChangeTerm(args.Term)
	rf.heartBeat = true

	reply.Term = rf.term
	reply.Success = true

	return
}

func (rf *Raft) StartVote() {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.voteNums.Store(1)
	rf.voteFore = rf.me
	rf.term += 1
	replyTerm := rf.term

	arg := RequestVoteArgs{Term: rf.term, CandidateId: rf.me, LastLogTerm: 0, LastLogIndex: 0}
	wg := sync.WaitGroup{}

	done := make(chan VoteState, 1)
	go func() {
		time.Sleep(time.Second)
		done <- VoteTimeOut
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		reply := RequestVoteReply{}
		go func(x int) {
			success := rf.sendRequestVote(x, &arg, &reply)
			if !success {
				fmt.Println("request rpc false")
			}

			if reply.VoteGranted == false {
				// need to change state to follower
				replyTerm = reply.Term
				done <- NeedToBecomeFollower
				return
			} else {
				rf.voteNums.Add(1)
				if rf.voteNums.Load() > uint32(len(rf.peers)/2) {
					done <- VoteForLeader
				}
			}

			wg.Done()
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
		rf.ChangeToFollower(replyTerm, rf.me)
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRpc(server int, args *AppendEntriesArgs, reply *AppendEntriesReplys) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntriesRpc", args, reply)
	return ok
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

	// Your code here (3B).

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

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()

		if rf.heartBeat == false && rf.state == FollowerState {
			rf.ChangeToCandidate()
		} else {
			rf.heartBeat = false
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) BrocastHeartBeat() {
	for rf.killed() == false {
		time.Sleep(50 * time.Millisecond)

		if rf.state == LeaderState {
			rf.mu.Lock()

			fmt.Println(fmt.Sprintf("now %d is leader", rf.me))
			arg := AppendEntriesArgs{Term: rf.term, LeaderId: rf.me, PreLogIndex: 0, Entries: make([]interface{}, 0), LeaderCommit: 0}
			reply := AppendEntriesReplys{}

			for i := range rf.peers {
				if rf.me != i {
					rf.sendAppendEntriesRpc(i, &arg, &reply)

					// if reply.term > this term return to follower
					if reply.Term > rf.term {
						rf.ChangeToFollower(reply.Term, -1)
						return
					}
				}
			}

			rf.mu.Unlock()
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

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.BrocastHeartBeat()

	return rf
}
