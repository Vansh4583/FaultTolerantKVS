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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab5/constants"
	"lab5/labgob"
	"lab5/labrpc"
	"lab5/logger"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log entry structure
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logger *logger.Logger
	// Your data here (4A, 4B, 4C).
	// Persistent state
	currentTerm int
	votedFor    int

	// Volatile state
	state string // "Follower", "Candidate", or "Leader"

	// Election timeout tracking
	electionResetTime time.Time

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	log         []LogEntry // each entry contains command and term
	nextIndex   []int      // for each server, index of the next log entry to send
	matchIndex  []int      // for each server, index of highest log entry known to be replicated
	commitIndex int
	lastApplied int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (4A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "Leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (4C).
	// Example:
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
	// Your code here (4C).
	// Example:
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return // or log error
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int // for redirecting clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // for leader to update itself
	Success bool // true if follower accepts the heartbeat
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateId  int
	LastLogIndex int //
	LastLogTerm  int //
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "Follower"
		rf.electionResetTime = time.Now()
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	// Election restriction: only vote for up-to-date candidates
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	candidateUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm &&
			args.LastLogIndex >= lastLogIndex)

	// Grant vote if candidate is up-to-date AND we haven't voted (or voting for same candidate)
	if candidateUpToDate &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.state != "Leader" {
		rf.votedFor = args.CandidateId
		rf.electionResetTime = time.Now()
		reply.VoteGranted = true
		rf.persist()
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
	// index := -1
	term := -1
	// isLeader := true

	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.state != "Leader" {
		return -1, rf.currentTerm, false
	}

	// Append command to leader's log
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	index := len(rf.log) - 1 // log is 1-based with dummy at index 0
	term = rf.currentTerm
	isLeader := true

	// Immediately try to replicate to followers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateLogTo(peer)
	}

	return index, term, isLeader
}

// Function which ensures replication of logs across all nodes
func (rf *Raft) replicateLogTo(peer int) {
	rf.mu.Lock()
	if rf.state != "Leader" {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	leaderCommit := rf.commitIndex

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	entries := make([]LogEntry, len(rf.log[rf.nextIndex[peer]:]))
	copy(entries, rf.log[rf.nextIndex[peer]:])

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.electionResetTime = time.Now()
		rf.persist()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[peer] = match
		rf.nextIndex[peer] = match + 1

		// Advance commitIndex if possible
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {

			//  Only commit entries from current term
			if rf.log[i].Term != rf.currentTerm {
				continue
			}

			count := 1 // include self
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}
			if count*2 > len(rf.peers) {
				rf.commitIndex = i
			}
		}
	} else {

		// AGGRESSIVE BACKUP
		if rf.nextIndex[peer] > 1 {
			// Use exponential backoff for very fast convergence
			oldNext := rf.nextIndex[peer]

			if oldNext > 100 {
				rf.nextIndex[peer] = oldNext / 2 // Cut in half for large logs
			} else if oldNext > 20 {
				rf.nextIndex[peer] = oldNext - 10 // Jump back by 10
			} else if oldNext > 5 {
				rf.nextIndex[peer] = oldNext - 3 // Jump back by 3
			} else {
				rf.nextIndex[peer]-- // Near beginning, go one by one
			}

			// Ensure we don't go below 1
			if rf.nextIndex[peer] < 1 {
				rf.nextIndex[peer] = 1
			}
		}
	}
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Always reset election timer (for both heartbeats and log entries)
	rf.electionResetTime = time.Now()

	// Handle term checks
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "Follower"
		rf.persist()
	}

	// Check log consistency
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogIndex < 0 {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Check if the log entry at PrevLogIndex has the right term
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Handle heartbeats (empty entries)
	if len(args.Entries) == 0 {
		// Update commit index if leader has committed more entries
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			if rf.commitIndex < 0 {
				rf.commitIndex = 0
			}
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	// Handle log replication (non-empty entries)
	insertIndex := args.PrevLogIndex + 1

	// Check for conflicts and handle them properly
	conflictIndex := -1
	for i := 0; i < len(args.Entries); i++ {
		logIndex := insertIndex + i
		if logIndex < len(rf.log) {
			// If there's a term mismatch, we found a conflict
			if rf.log[logIndex].Term != args.Entries[i].Term {
				conflictIndex = logIndex
				break
			}
		} else {
			// We've reached the end of our log - no conflict, just need to append
			break
		}
	}

	// If we found a conflict, truncate the log from the conflict point
	if conflictIndex != -1 {
		rf.log = rf.log[:conflictIndex]
		rf.persist()
	}

	logAppended := false

	// Append all new entries that aren't already in our log
	for i := 0; i < len(args.Entries); i++ {
		logIndex := insertIndex + i
		if logIndex >= len(rf.log) {
			// This entry is beyond our current log, so append it
			rf.log = append(rf.log, args.Entries[i])
			logAppended = true
		}
		// If logIndex < len(rf.log), the entry already matches (checked above)
	}

	if logAppended {
		rf.persist()
	}

	//
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommitIndex := min(args.LeaderCommit, lastNewIndex)

		// Only advance commitIndex to entries we actually have
		if newCommitIndex < len(rf.log) {
			rf.commitIndex = newCommitIndex
		}

		if rf.commitIndex < 0 {
			rf.commitIndex = 0
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go func(peer int) {
				rf.mu.Lock()
				prevLogIndex := len(rf.log) - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				// HEARTBEAT: Empty entries
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []LogEntry{}, // EMPTY for heartbeats
					LeaderCommit: leaderCommit,
				}
				rf.mu.Unlock()

				var reply AppendEntriesReply
				ok := rf.peers[peer].Call("Raft.AppendEntries", args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.state = "Follower"
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.electionResetTime = time.Now()
					rf.persist()
					return
				}

			}(peer)
		}

		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (4A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		// Sleep for a small fixed amount
		rf.mu.Lock()
		timeout := 300 + rand.Intn(150) // 300–450ms randomized timeout
		if rf.state != "Leader" && time.Since(rf.electionResetTime) >= time.Duration(timeout)*time.Millisecond {
			go rf.startElection()
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// Step 1: Become candidate
	rf.currentTerm += 1
	rf.state = "Candidate"
	rf.votedFor = rf.me
	rf.persist()
	termStarted := rf.currentTerm
	rf.electionResetTime = time.Now()
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	rf.mu.Unlock()

	rf.logger.Log(constants.LogVote, "Starting election for term %d", termStarted)

	votesReceived := 1
	var muVotes sync.Mutex
	var wg sync.WaitGroup

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		wg.Add(1)
		go func(peer int) {
			defer wg.Done()

			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.logger.Log(constants.LogVote, "Found higher term %d from peer %d", reply.Term, peer)
					rf.currentTerm = reply.Term
					rf.state = "Follower"
					rf.votedFor = -1
					rf.electionResetTime = time.Now()
					return
				}

				if rf.state == "Candidate" && reply.Term == termStarted && reply.VoteGranted {
					muVotes.Lock()
					votesReceived++
					muVotes.Unlock()

					if votesReceived > len(rf.peers)/2 {
						rf.logger.Log(constants.LogLeader, "Elected leader for term %d", termStarted)
						rf.state = "Leader"

						// Initialize leader state
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log) // Start with leader's log length
							rf.matchIndex[i] = 0          // Reset match index
						}
						go rf.sendHeartbeats()
					}
				}
			}
		}(peer)
	}

	wg.Wait() // Wait for RPCs to finish before returning
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
	labgob.Register(LogEntry{})
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.logger = logger.NewLogger(me+1, true, fmt.Sprintf("raft-%d", me), constants.RaftLoggingMap)

	// Your initialization code here (4A, 4B, 4C).
	rf.state = "Follower"
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.electionResetTime = time.Now()
	rf.log = []LogEntry{{Term: 0}}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		// matchIndex defaults to 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logger.Log(constants.LogRaftStart, "Raft server started")

	rf.applyCh = applyCh // Store the channel

	// Start goroutines
	go rf.ticker()
	go rf.applyLogs()

	return rf
}

// Applying the log entry
func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()

		// Apply committed entries that haven't been applied yet
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied == 0 {
				continue // Skip dummy entry at index 0
			}

			// Make sure we don't go out of bounds
			if rf.lastApplied >= len(rf.log) {
				break
			}

			entry := rf.log[rf.lastApplied]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg // Send to service layer
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
