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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

const (
	Leader    = 0
	Follower  = 1
	Candidate = 2
)

type LogEntry struct {
	Message interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	applyLock   sync.Mutex

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other auxiliary states
	state       int
	applyCh     chan ApplyMsg
	winElectCh  chan bool
	stepDownCh  chan bool
	heartbeatCh chan bool
	grantVoteCh chan bool
}

func asyncChanIn(ch chan<- bool, val bool) {
	go func(ch chan<- bool) {
		ch <- val
	}(ch)
}

func (rf *Raft) initializePeerInfo() {
	rf.nextIndex = make([]int, rf.GetNumberOfPeers())
	rf.matchIndex = make([]int, rf.GetNumberOfPeers())
	for i := 0; i < rf.GetNumberOfPeers(); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex[rf.me] = len(rf.logs) - 1
}

//Lock required
func (rf *Raft) Clone() Raft {
	return Raft{
		me:    rf.me,
		dead:  rf.dead,
		peers: rf.peers,

		currentTerm: rf.currentTerm,
		votedFor:    rf.votedFor,
		logs:        rf.logs,
		commitIndex: rf.commitIndex,
		lastApplied: rf.lastApplied,

		nextIndex:  rf.nextIndex,
		matchIndex: rf.matchIndex,

		state:       rf.state,
		applyCh:     rf.applyCh,
		winElectCh:  rf.winElectCh,
		stepDownCh:  rf.stepDownCh,
		heartbeatCh: rf.heartbeatCh,
		grantVoteCh: rf.grantVoteCh,
	}
}
func (rf *Raft) GetLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) GetNumberOfPeers() int {
	return len(rf.peers)
}

//Lock required
func (rf *Raft) SetTerm(term int) {
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.ResetChannel()
	}
}
func (rf *Raft) ResetChannel() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
}

//Lock required
func (rf *Raft) ConvertToLeader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.mu.Unlock()
	rf.BroadCastAppendEntry()
	// fmt.Printf("I am leader %d, term %d\n", rf.me, rf.currentTerm)
}

//unlock required
func (rf *Raft) ConvertToFollower(term int) {
	rf.mu.Lock()
	state := rf.state
	rf.state = Follower
	rf.SetTerm(term)
	rf.votedFor = -1
	// step down if not follower, this check is needed
	// to prevent race where state is already follower
	if state != Follower {
		rf.mu.Unlock()
		asyncChanIn(rf.stepDownCh, true)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) ConvertToCandidate(from int) {
	rf.mu.Lock()
	if from != rf.state {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.SetTerm(rf.currentTerm + 1)
	rf.StartElection(rf.currentTerm)
	rf.mu.Unlock()
}
func (rf *Raft) PrintLog() {
	for index, i := range rf.logs {
		fmt.Printf("Server %d index %d: %d\n", rf.me, index, i)
	}
}

//Lock required
func (rf *Raft) StartElection(term int) {
	if rf.state != Candidate {
		return
	}
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastIndex(),
		LastLogTerm:  rf.logs[rf.GetLastIndex()].Term,
	}
	ch := make(chan bool)
	// fmt.Printf("Server %d start election on term %d\n", rf.me, rf.currentTerm)
	rf.persist()
	//copy state and use the copy
	copy := rf.Clone()
	for i := 0; i < rf.GetNumberOfPeers(); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, &RequestVoteReply{}, ch)
	}
	go func() {
		count := 1
		for i := 1; i < copy.GetNumberOfPeers(); i++ {
			if <-ch {
				count++
			}
			if count > rf.GetNumberOfPeers()/2 {
				fmt.Printf("Server %d win term %d\n", copy.me, copy.currentTerm)
				asyncChanIn(copy.winElectCh, true)
				return
			}
		}
	}()
	// fmt.Println("finished")
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = rf.state == Leader
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logs) != nil {
		panic("Encode persister error")
	}
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil {
		panic("Decode persister error")
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Succeed bool
	Term    int
}

type RequestAppendEntryArgs struct {
	Term          int
	LeaderId      int
	PreviousIndex int
	PreviousTerm  int
	LeaderCommit  int
	Entries       []LogEntry
}

type RequestAppendEntryReply struct {
	Succeed    bool
	Term       int
	LastCommit int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("Server %d recieved request vote from %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Succeed = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		rf.persist()
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.ConvertToFollower(args.Term)
		rf.mu.Lock()
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUptodate(args) {
		reply.Succeed = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
	}
	rf.mu.Unlock()
	rf.persist()
}
func (rf *Raft) isUptodate(args *RequestVoteArgs) bool {
	if rf.logs[rf.GetLastIndex()].Term == args.LastLogTerm {
		return args.LastLogIndex >= rf.GetLastIndex()
	}
	return rf.logs[rf.GetLastIndex()].Term < args.LastLogTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan<- bool) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
	}
	asyncChanIn(ch, ok && reply.Succeed)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	rf.mu.Unlock()
	index := len(rf.logs) - 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (2B).
	if !isLeader {
		rf.persist()
		return index, term, isLeader
	}
	// fmt.Println(command)
	rf.logs = append(rf.logs, LogEntry{command, term})
	index = len(rf.logs) - 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	return index, term, isLeader
}

func (rf *Raft) BroadCastAppendEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	for i := 0; i < rf.GetNumberOfPeers(); i++ {
		if i == rf.me {
			continue
		}
		preIndex := rf.nextIndex[i] - 1
		args := RequestAppendEntryArgs{
			Term:          rf.currentTerm,
			LeaderId:      rf.me,
			PreviousIndex: preIndex,
			PreviousTerm:  rf.logs[preIndex].Term,
			Entries:       rf.logs[preIndex+1 : len(rf.logs)],
			LeaderCommit:  rf.commitIndex,
		}
		go rf.SendRequestAppendEntry(i, &args, &RequestAppendEntryReply{})
	}
}
func (rf *Raft) SendRequestAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.currentTerm != args.Term {
		rf.persist()
		return ok
	}
	if !ok {
		rf.persist()
		return ok
	}
	if reply.Succeed {
		newIndex := args.PreviousIndex + len(args.Entries)
		if newIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.UpdateCommit(rf.matchIndex[server])
	} else {
		rf.matchIndex[server] = reply.LastCommit
		rf.nextIndex[server] = reply.LastCommit + 1
	}
	rf.persist()
	return ok
}

//Lock required
func (rf *Raft) UpdateCommit(index int) {
	sorted := make([]int, rf.GetNumberOfPeers())
	for i := 0; i < len(sorted); i++ {
		sorted[i] = rf.matchIndex[i]
	}
	for i := 0; i < len(sorted); i++ {
		for j := 1; j < len(sorted)-i; j++ {
			if sorted[j] < sorted[j-1] {
				temp := sorted[j]
				sorted[j] = sorted[j-1]
				sorted[j-1] = temp
			}
		}
	}
	rf.commitIndex = sorted[rf.GetNumberOfPeers()/2]
	go rf.ApplyLogs()
	// if index <= rf.commitIndex {
	// 	return
	// }
	// count := 0
	// for i := 0; i < rf.GetNumberOfPeers(); i++ {
	// 	if rf.matchIndex[i] >= index {
	// 		count++
	// 	}
	// }
	// if count > rf.GetNumberOfPeers()/2 {
	// 	rf.commitIndex = index
	// 	go rf.ApplyLogs()
	// }
}

//in go routine
func (rf *Raft) ApplyLogs() {
	rf.applyLock.Lock()
	defer rf.applyLock.Unlock()
	for ; rf.lastApplied < rf.commitIndex; rf.lastApplied++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied+1].Message,
			CommandIndex: rf.lastApplied + 1,
		}
	}
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	// fmt.Printf("recieve heart beat %d\n", rf.me)
	rf.mu.Lock()
	// fmt.Printf("recieve heart beat %d\n", rf.me)

	// if len(args.Entries) > 0 {
	// 	fmt.Printf("Recieved log %d at sever %d as %d\n", args.Entries[0], rf.me, rf.state)
	// }
	if rf.currentTerm > args.Term {
		reply.Succeed = false
		reply.Term = rf.currentTerm
		reply.LastCommit = rf.commitIndex
		rf.mu.Unlock()
		rf.persist()
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.ConvertToFollower(args.Term)
		rf.mu.Lock()
	}
	rf.mu.Unlock()

	go func() { rf.heartbeatCh <- true }()
	//2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.PreviousIndex < len(rf.logs) && rf.logs[args.PreviousIndex].Term == args.PreviousTerm {
		reply.Succeed = true
		reply.Term = rf.currentTerm
		rf.logs = rf.logs[0 : args.PreviousIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	} else {
		reply.Succeed = false
		reply.Term = rf.currentTerm
		reply.LastCommit = rf.commitIndex
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.logs) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
		// fmt.Printf("Server %d has applied the copy command\n", rf.me)
		go rf.ApplyLogs()
	}
	rf.persist()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func randtime() int {
	return 150 + rand.Intn(150)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// fmt.Printf("Server %d runs %d\n", rf.me, rf.state)
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(50 * time.Millisecond):
				// fmt.Printf("send heatbeat %d\n", rf.me)
				rf.BroadCastAppendEntry()
			}
		case Follower:
			select {
			case <-rf.stepDownCh:
			case <-rf.heartbeatCh:
			case <-rf.grantVoteCh:
			case <-time.After(1000 * time.Millisecond):
				rf.ConvertToCandidate(state)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.heartbeatCh:
				rf.ConvertToFollower(rf.currentTerm)
			case <-time.After(time.Duration(randtime()) * time.Millisecond):
				rf.ConvertToCandidate(state)
			case <-rf.winElectCh:
				rf.ConvertToLeader()
			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.SetTerm(0)
	rf.state = Follower
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Term: 0}
	rf.initializePeerInfo()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
