package raft

import (
	"6.5840/labgob"
	"bytes"
	"strconv"
	"strings"

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

type LOG []Entry

type RVC struct {
	reply  RequestVoteReply
	server int
}

func (log LOG) terms() []int {
	terms := make([]int, len(log))
	for i := range log {
		terms[i] = log[i].Term
	}
	return terms
}

func stringify(l []int) []string {
	a := make([]string, len(l))
	for i := range l {
		a[i] = strconv.Itoa(l[i])
	}
	return a
}

func (rf *Raft) terms() string {
	terms := stringify(rf.log.terms())
	terms[rf.commitIndex] += "*"
	terms[rf.lastApplied] += "*"
	return "|" + strings.Join(terms, "|") + "|"
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         LOG

	commitIndex int // index of the highest entry known to be commited
	lastApplied int // index of the highest entry applied

	nextIndex  []int
	matchIndex []int

	state   int // 0 for follower, 1 for candidate, 2 for leader
	timeout int64

	applyCh chan ApplyMsg
	votes   int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int  // current Term for candidate to update itself
	VoteGranted bool // true means the candidate received vote
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == 2
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

	Debug(dWarn, rf.format("%d killed"), rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) format(in string) string {
	if rf.state == 0 {
		return "F" + in
	} else if rf.state == 1 {
		return "C" + in
	} else {
		return "L" + in
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil {
		Debug(dWarn, "S%d read persist fail", rf.me)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = args.Term
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d reject1 S%d AT%d", rf.me, args.CandidateID, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		Debug(dTerm, rf.format("%d --> F%d"), rf.me, rf.me)
		rf.state = 0
		rf.votedFor = -1
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		Debug(dVote, "S%d reject3 S%d AT%d", rf.me, args.CandidateID, args.Term)
		return
	}
	index, term := len(rf.log)-1, rf.log[len(rf.log)-1].Term
	if args.LastLogTerm < term {
		Debug(dVote, "S%d reject4 S%d AT%d", rf.me, args.CandidateID, args.Term)
		return
	}
	if args.LastLogTerm == term && args.LastLogIndex < index {
		Debug(dVote, "S%d reject5 S%d AT%d", rf.me, args.CandidateID, args.Term)
		return
	}
	Debug(dVote, "F%d vote S%d AT%d", rf.me, args.CandidateID, args.Term)
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 {
		Debug(dTimer, "S%d <-[]- L%d T%d", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dInfo, "S%d <-[%d:%d]- L%d T%d", rf.me, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries),
			args.LeaderId,
			rf.currentTerm)
	}
	if args.Term < reply.Term {
		return
	}
	if args.Term > reply.Term {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.votedFor = -1
	}
	rf.refresh()
	if args.PrevLogIndex >= len(rf.log) {
		return
	}
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		return
	}
	rf.refresh()
	reply.Success = true
	rf.currentTerm = args.Term
	rf.state = 0
	rf.votedFor = -1
	j := 0
	for i := args.PrevLogIndex + 1; i < len(rf.log) && j < len(args.Entries); i++ {
		rf.log[i] = args.Entries[j]
		j++
	}
	rf.log = append(rf.log, args.Entries[j:]...)
	if args.LeaderCommit > rf.commitIndex {
		mini := func(a int, b int) int {
			if a > b {
				return b
			}
			return a
		}
		rf.commitIndex = mini(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
	go rf.apply()
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) == 0 {
		Debug(dTimer, "L%d -[]-> S%d", rf.me, server)
	} else {
		Debug(dInfo, "L%d -[%d:%d]-> S%d", rf.me, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), server)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	if rf.state != 2 {
		return index, rf.currentTerm, false
	}
	entry := Entry{rf.currentTerm, command}
	rf.log = append(rf.log, entry)
	format := "L%d CMD" + rf.terms() + " AT%d"
	Debug(dLeader, format, rf.me, rf.currentTerm)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	return index, rf.currentTerm, true
}

func (rf *Raft) stepBack(server int) {
	rf.mu.Lock()
	if rf.nextIndex[server] <= 1 {
		rf.mu.Unlock()
		return
	}
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
	if !reply.Success {
		go rf.stepBack(server)
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	RPC := make(chan struct {
		reply   AppendEntriesReply
		server  int
		matchTo int
	}, len(rf.peers))
	requests := make([]AppendEntriesArgs, len(rf.peers))
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		requests[server] = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
	}
	rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			_ = rf.sendAppendEntries(server, args, reply)
			RPC <- struct {
				reply   AppendEntriesReply
				server  int
				matchTo int
			}{*reply, server, args.PrevLogIndex + len(args.Entries)}
		}(server, &requests[server], &AppendEntriesReply{})
	}
	for i := 1; i < len(rf.peers); i++ {
		r := <-RPC
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		reply := r.reply
		server := r.server
		var f string
		if reply.Success {
			f = "O"
		} else {
			f = "X"
		}
		Debug(dInfo, "S%d <-"+f+"*- S%d", rf.me, server)
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.nextIndex[server] = r.matchTo + 1
			rf.matchIndex[server] = r.matchTo
			go rf.commit()
		} else {
			go rf.stepBack(server)

		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		if rf.log[i].Term < rf.currentTerm {
			continue
		}
		count := 0
		for s := 0; s < len(rf.peers); s++ {
			if rf.matchIndex[s] >= i {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			rf.commitIndex = i
			format := rf.format("%d commit" + rf.terms() + " AT%d")
			Debug(dLog, format, rf.me, rf.currentTerm)
		} else {
			break
		}
	}
	go rf.apply()
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	msgs := make([]ApplyMsg, 0)
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[i].Command,
				CommandIndex:  i,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			})
		}
		rf.lastApplied = rf.commitIndex
		Debug(dLog, rf.format("%d apply %d AT%d"), rf.me, rf.lastApplied, rf.currentTerm)
	}
	rf.mu.Unlock()
	for _, msg := range msgs {
		rf.applyCh <- msg
	}
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	rf.currentTerm++
	Debug(dTerm, rf.format("%d -> C%d"), rf.me, rf.me)
	rf.state = 1 // convert to candidate
	rf.votedFor = rf.me
	rf.votes = 1
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	RPC := make(chan RVC, len(rf.peers))
	rf.mu.Unlock()
	go rf.countVotes(RPC)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			Debug(dInfo, "C%d -RV-> S%d", rf.me, server)
			_ = rf.sendRequestVote(server, args, reply)
			RPC <- RVC{*reply, server}
		}(server, &args, &RequestVoteReply{})
	}
}

func (rf *Raft) countVotes(RPC chan RVC) {
	for i := 1; i < len(rf.peers); i++ {
		r := <-RPC
		reply := r.reply
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			Debug(dTerm, rf.format("%d -> F%d"), rf.me, rf.me)
			rf.state = 0
			rf.votedFor = -1
			rf.refresh()
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if reply.Term < rf.currentTerm || rf.state != 1 {
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			rf.votes++
			Debug(dInfo, "C%d <-V*- S%d T%d", rf.me, r.server, rf.currentTerm)
		}
		if rf.votes*2 >= len(rf.peers) {
			Debug(dLeader, rf.format("%d -> L%d AT%d"), rf.me, rf.me, rf.currentTerm)
			rf.state = 2
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.persist()
			go rf.heartbeat()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) refresh() {
	rf.timeout = time.Now().UnixMilli() + 110 + (rand.Int63() % 300)
}

func (rf *Raft) ticker() {
	if rf.killed() {
		return
	}
	time.Sleep(time.Duration(50+rand.Int()%50) * time.Millisecond)
	go rf.ticker()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	format := rf.format("%d T%d ->%d " + rf.terms() + " C%d A%d -%d")
	Debug(dTimer, format, rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex,
		rf.lastApplied, rf.timeout-time.Now().UnixMilli())
	if rf.state == 2 {
		go rf.heartbeat()
	}
	if rf.timeout > time.Now().UnixMilli() {
		return
	}
	rf.refresh()
	if rf.state == 1 || (rf.state == 0 && rf.votedFor == -1) {
		format := rf.format("%d timeout AT%d")
		Debug(dTimer, format, rf.me, rf.currentTerm)
		go rf.electLeader()
		return
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]Entry, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		state:       0,
		timeout:     0,
		applyCh:     applyCh,
		votes:       0,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.refresh()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.votedFor == rf.me {
		rf.state = 1
	}
	Debug(dClient, "S%d T%d ->%d [%d] C%d A%d -%d init", rf.me, rf.currentTerm, rf.votedFor, len(rf.log),
		rf.commitIndex, rf.lastApplied, rf.timeout-time.Now().UnixMilli())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
