package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
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

type Entry struct {
	Term    int
	Command interface{}
}

const MAXRETYR int = 10

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
	currentTerm int32
	votedFor    int
	log         []Entry

	commitIndex int // index of the highest entry known to be commited
	lastApplied int // index of the highest entry applied

	nextIndex  []int
	matchIndex []int

	state   int // 0 for follower, 1 for candidate, 2 for leader
	timeout int64

	applyCh chan ApplyMsg
}

type AppendEntriesArgs struct {
	Term     int32
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // current Term for candidate to update itself
	VoteGranted bool  // true means the candidate received vote
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == 2
	rf.mu.Unlock()
	return int(term), isleader
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
	Debug(dWarn, "S%d was killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < reply.Term {
		return
	}
	if args.Term == reply.Term && rf.state == 2 {
		return
	}
	if args.Term == reply.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		return
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
		return
	}
	rf.currentTerm = args.Term
	Debug(dVote, "S%d vote %d at T%d", rf.me, args.CandidateID, args.Term)
	rf.state = 0
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d <-[:]- L%d T%d", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dInfo, "S%d <-[%d:%d]- L%d T%d", rf.me, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries),
			args.LeaderId,
			rf.currentTerm)
	}
	if args.Term < reply.Term {
		return
	}
	if args.PrevLogIndex >= len(rf.log) {
		return
	}
	if args.PrevLogTerm != rf.log[len(rf.log)-1].Term {
		return
	}
	rf.refresh()
	reply.Success = true
	rf.currentTerm = args.Term
	rf.state = 0
	rf.votedFor = -1
	c := 0
	for i := args.PrevLogIndex + 1; i < len(rf.log) && c < len(args.Entries); i++ {
		rf.log[i] = args.Entries[c]
		c++
	}
	rf.log = append(rf.log, args.Entries[c:]...)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
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
	Debug(dInfo, "S%d -RV-> S%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d -[:]-> S%d", rf.me, server)
	} else {
		Debug(dInfo, "S%d -[%d:%d]-> S%d", rf.me, args.PrevLogIndex+1, args.PrevLogTerm+len(args.Entries), server)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	// Your code here (2B).
	if rf.state != 2 {
		return index, int(rf.currentTerm), false
	}
	entry := Entry{int(rf.currentTerm), command}
	Debug(dLeader, "S%d got cmd %d at T%d", rf.me, len(rf.log), rf.currentTerm)
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	return index, int(rf.currentTerm), true
}

func (rf *Raft) stepBack(server int) {
	rf.mu.Lock()
	rf.nextIndex[server]--
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      rf.log[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	times := 1
	for ok := rf.sendAppendEntries(server, &args, &reply); !ok; times++ {
		if times > 5 {
			return
		}
		ok = rf.sendAppendEntries(server, &args, &reply)
	}
	rf.mu.Lock()
	if !reply.Success && rf.nextIndex[server] > 1 {
		go rf.stepBack(server)
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartbeat() {
	RPC := make(chan struct {
		reply   AppendEntriesReply
		server  int
		matchTo int
	}, len(rf.peers))
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			_ = rf.sendAppendEntries(server, args, reply)
			RPC <- struct {
				reply   AppendEntriesReply
				server  int
				matchTo int
			}{*reply, server, args.PrevLogIndex + len(args.Entries)}
		}(server, &args, &AppendEntriesReply{})
	}
	for i := 1; i < len(rf.peers); i++ {
		r := <-RPC
		rf.mu.Lock()
		reply := r.reply
		server := r.server
		var f string
		if reply.Success {
			f = "O"
		} else {
			f = "X"
		}
		Debug(dInfo, "S%d <-"+f+"*- S%d", rf.me)
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.nextIndex[server] = r.matchTo + 1
			rf.matchIndex[server] = r.matchTo
			go rf.commit()
		} else if rf.nextIndex[server] > 1 {
			go rf.stepBack(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		count := 0
		for s := 0; s < len(rf.peers); s++ {
			if rf.matchIndex[s] >= i {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			Debug(dLog, "S%d commit log %d at T%d", rf.me, i, rf.currentTerm)
			rf.commitIndex = i
		} else {
			Debug(dLog, "S%d commit [%d] fail %d", rf.me, i, count)
			break
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[i].Command,
				CommandIndex:  i,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
		}
		rf.lastApplied = rf.commitIndex
		Debug(dLog, "S%d apply log %d at T%d", rf.me, rf.lastApplied, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.state = 1 // convert to candidate
	rf.votedFor = rf.me
	logIndex := len(rf.log) - 1
	logTerm := rf.log[logIndex].Term
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: logIndex,
		LastLogTerm:  logTerm,
	}
	rf.mu.Unlock()

	vote := 1
	RPC := make(chan RequestVoteReply, len(rf.peers))

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			_ = rf.sendRequestVote(server, args, reply)
			RPC <- *reply
		}(server, &args, &RequestVoteReply{})
	}

	for i := 1; i < len(rf.peers); i++ {
		reply := <-RPC
		rf.mu.Lock()
		if term < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		Debug(dInfo, "S%d <-RV*- %v", rf.me, reply)
		if reply.VoteGranted {
			vote++
		}
		if vote*2 >= len(rf.peers) {
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
		if rf.state != 1 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if vote*2 <= len(rf.peers) {
		rf.mu.Unlock()
		return
	}
	Debug(dLeader, "S%d elected at T%d", rf.me, rf.currentTerm)
	rf.state = 2
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.votedFor = -1
	rf.mu.Unlock()
	go rf.heartbeat()
}

func (rf *Raft) refresh() {
	rf.timeout = time.Now().UnixMilli() + 110 + (rand.Int63() % 300)
}

func (rf *Raft) ticker() {
	if rf.killed() {
		return
	}
	// Your code here (2A)
	time.Sleep(time.Duration(100) * time.Millisecond)
	go rf.ticker()
	go rf.apply()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dTimer, "S%d [%d] 𝚫%d T%d", rf.me, rf.lastApplied, rf.timeout-time.Now().UnixMilli(), rf.currentTerm)
	if rf.state == 2 {
		go rf.heartbeat()
	}
	if rf.timeout > time.Now().UnixMilli() {
		return
	}
	rf.refresh()
	if rf.state == 1 || (rf.state == 0 && rf.votedFor == -1) {
		Debug(dTimer, "S%d timeout at T%d", rf.me, rf.currentTerm)
		go rf.electLeader()
		return
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = 0 // follower
	rf.applyCh = applyCh
	rf.refresh()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
