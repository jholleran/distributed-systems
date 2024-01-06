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
	"6.5840/labgob"
	"bytes"
	"log"
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

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	applyCh            chan ApplyMsg
	newCommitReadyChan chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state string

	lastReceive time.Time
	// persistent
	currentTerm int
	votedFor    int

	log []LogEntry
	// volatile
	commitIndex int

	lastApplied int
	// volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

const (
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

var EMPTY_ENTRY = LogEntry{Command: nil, Term: -1, Index: 0}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.currentTerm
	var isleader = rf.state == Leader
	DPrintf("[%d] state check - term %d, state %v", rf.me, rf.currentTerm, rf.state)

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.commitIndex)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.commitIndex) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("Unable to read raft state.")
	} else {
		rf.currentTerm = rf.log[len(rf.log) - 1].Term
		rf.lastApplied = rf.commitIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 5.4.1 - Election restriction
	// only vote yes if candidate has a higher term in last entry, or same last term and greater than or equal to log index

	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex != 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	//log.Printf("[%d] RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", rf.me, args, rf.currentTerm, rf.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > rf.currentTerm {
		//log.Printf("[%d]... term out of date in RequestVote", rf.me)
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		// reset election timer
		rf.lastReceive = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	rf.persist()

	//log.Printf("[%d]... RequestVote reply: %+v", rf.me, reply)
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term             int
	LeaderId         int
	PreviousLogIndex int
	PreviousLogTerm  int
	Entries          []LogEntry
	LeaderCommit     int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	// if a candidate and have received AppendEntries from new leader: convert to follower

	// AppendEntries will only every be called by an elected leader


	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("[%d] AppendEntries request: %+v", rf.me, args)

	// reject if term in arguments is less then the current term
	// if RPC request contains term T > currentTerm: set currentTerm = T, convert to follower

	if args.Term > rf.currentTerm {
		DPrintf("[%d]... term out of date in AppendEntries. currentTerm := %d, incoming term := %d", rf.me, rf.currentTerm, args.Term)
		rf.becomeFollower(args.Term)
	}

	reply.Success = false

	if rf.currentTerm == args.Term {
		//log.Printf("[%d]... state is: %v", rf.me, rf.state)
		if rf.state != Follower {
			DPrintf("[%d]... in same term but not a follower", rf.me)
			rf.becomeFollower(args.Term)
		}

		// reset election timer
		rf.lastReceive = time.Now()

		//HeartbeatPrintf("[%d] received heartbeat from %d in term: %v", rf.me, args.LeaderId, rf.currentTerm)

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1 this is
		// vacuously true.
		if args.PreviousLogIndex == -1 ||
			(args.PreviousLogIndex < len(rf.log) &&
				args.PreviousLogTerm == rf.log[args.PreviousLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PreviousLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				//log.Printf("[%d]... inserting entries %v from index %d", rf.me, args.Entries[newEntriesIndex:], logInsertIndex)
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				DPrintf("[%d]... log is now: %v", rf.me, rf.log)
			}

			// Set commit index.
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = intMin(args.LeaderCommit, len(rf.log)-1)
				DPrintf("[%d]... setting commitIndex=%d", rf.me, rf.commitIndex)
				rf.newCommitReadyChan <- struct{}{}
			}
		}

		reply.Term = rf.currentTerm
		rf.persist()
		//log.Printf("[%d] AppendEntries request: %+v, reply: %+v", rf.me, *args, *reply)
		//log.Printf("[%d] AppendEntries reply: %+v", rf.me, *reply)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//
	if rf.state == Leader {
		//log.Printf("[%d] received start command: %v", rf.me, command)
		isLeader = true
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm, Index: index})

		rf.persist();
		//log.Printf("[%d]... log=%v", rf.me, rf.log)
	}

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

func (rf *Raft) sendHeartbeatToAll() {

	for rf.killed() == false {

		// pause for 100
		time.Sleep(time.Duration(100) * time.Millisecond)

		rf.mu.Lock()
		term := rf.currentTerm
		state := rf.state
		rf.mu.Unlock()


		if state != Leader {
			return
		}

		DPrintf("[%d] going to send heartbeat append entries to all peers for term: %d", rf.me, term)

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(peer int, term int) {
				rf.mu.Lock()
				ni := rf.nextIndex[peer]

				// Not sure about this. We need to make sure the initial entry is not added to entries.
				//if len(rf.log) > 1 && ni == 0 {
				//	ni = ni + 1
				//}


				prevLogIndex := ni - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				entries := rf.log[ni:]

				args := &AppendEntriesArgs{
					Term:             term,
					LeaderId:         rf.me,
					PreviousLogIndex: prevLogIndex,
					PreviousLogTerm:  prevLogTerm,
					Entries:          entries,
					LeaderCommit:     rf.commitIndex,
				}
				rf.mu.Unlock()


				//log.Printf("[%d] sending AppendEntries to: %d: ni=%d", rf.me, peer, ni)
				//log.Printf("[%d] sending AppendEntries to: %d: ni=%d, args:%+v", rf.me, peer, ni, args)

				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(peer, args, reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				//log.Printf("[%d] state := %v, current term is %d and reply term is %d, success := %v,", rf.me, rf.state, term, reply.Term, reply.Success)
				if reply.Term > term {
					AppendEntriesPrintf("[%d] term out of date in heartbeat reply", rf.me)
					rf.becomeFollower(reply.Term)
					return
				}

				if rf.state == Leader && term == reply.Term {
					if reply.Success {
						rf.nextIndex[peer] = ni + len(entries)
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
						AppendEntriesPrintf("[%d] AppendEntries reply from: %d success: nextIndex := %v, matchIndex := %v", rf.me, peer, rf.nextIndex, rf.matchIndex)

						savedCommitIndex := rf.commitIndex
						for i := rf.commitIndex + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == rf.currentTerm {
								matchCount := 1
								for peerIdBadName, _ := range rf.peers {
									if peerIdBadName == rf.me {
										continue
									}
									if rf.matchIndex[peerIdBadName] >= i {
										matchCount++
									}
									if matchCount*2 > len(rf.peers) {
										DPrintf("[%d] leader has replicated the entry on a majority.  commitIndex := %d, nodes := %d", rf.me, i, matchCount)
										rf.commitIndex = i
									}
								}
							}
						}

						if rf.commitIndex != savedCommitIndex {
							rf.newCommitReadyChan <- struct{}{}
						}
					} else {
						rf.nextIndex[peer] = ni - 1
						AppendEntriesPrintf("[%d] AppendEntries reply from: %d !success: nextIndex := %d", rf.me, peer, rf.nextIndex[peer])
					}
				}

			}(server, term)
		}

	}

}

func (rf *Raft) commitChanSender() {
	for range rf.newCommitReadyChan {

		rf.mu.Lock()

		// Find which entries we have to apply.
		savedLastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}

		DPrintf("[%d] commitChanSender entries=%v, savedLastApplied=%d", rf.me, entries, savedLastApplied)
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: savedLastApplied + i + 1,
				CommandValid: true,
			}
		}
	}
	DPrintf("commitChanSender done")
}

func attemptElection(rf *Raft) {
	rf.mu.Lock()

	// we are now a candidate
	rf.state = Candidate

	// increment current term
	//rf.currentTerm++

	// vote for self
	rf.votedFor = rf.me

	// reset election timer
	rf.lastReceive = time.Now()

	// store a temp copy of the term
	term := rf.currentTerm

	// increment the term
	term++

	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex != 0 && len(rf.log) > 1 {
		//log.Printf("%v", rf.log)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	votes := 1
	done := false

	rf.persist()

	ElectionPrintf("[%d] attempting an election at term %d", rf.me, term)
	rf.mu.Unlock()

	// send RequestVote RPC to all other servers

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(peer int, term int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(peer, &RequestVoteArgs{CandidateId: rf.me, Term: term, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > term {
				log.Printf("[%d] term is out of date in reply. save term:%d, %+v", rf.me, term, reply)
				rf.becomeFollower(reply.Term)
				return
			}

			if !reply.VoteGranted {
				return
			}

			votes++
			ElectionPrintf("[%d] got vote from: %d", rf.me, peer)

			// check if we have enough votes
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true

			// check start again in case we are no longer a candidate
			if rf.state != Candidate {
				log.Printf("[%d] while waiting for reply state is no longer a Candidate. state=%s", rf.me, rf.state)
				return
			}

			rf.startLeader(term)

		}(server, term)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		startTime := time.Now()
		// pause for a random amount of time between 700 and 1000 milliseconds.
		ms := 700 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()

		if rf.state != Leader &&
			rf.lastReceive.Before(startTime) {
			// haven't heard from the leader attempt an election

			go attemptElection(rf)
		}

		rf.mu.Unlock()
	}
}

// this will change state so locks must be in place before calling this
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	//log.Printf("[%d] is now a Follower in term=%d, log=%v", rf.me, rf.currentTerm, rf.log)

	// reset election timer
	rf.lastReceive = time.Now()
}

// this will change state so locks must be in place before calling this
func (rf *Raft) startLeader(term int) {
	rf.state = Leader
	rf.currentTerm = term

	log.Printf("[%d] is now the Leader in term: %d", rf.me, rf.currentTerm)

	go rf.sendHeartbeatToAll()
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
	rf.state = Follower
	rf.lastReceive = time.Now()

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.applyCh = applyCh
	rf.newCommitReadyChan = make(chan struct{}, 16000) // HACK for now but this causes locking

	// Initialize the first entry to empty
	rf.log = append(rf.log, EMPTY_ENTRY)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start the goroutine to start commit channel sender
	go rf.commitChanSender()

	return rf
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
