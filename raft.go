package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Heartbeat and election timeouts; the timeout window sits well
// above heartbeatInterval so followers don't time out early.
const (
	heartbeatInterval    = 100 * time.Millisecond
	electionTimeoutMin   = 300 * time.Millisecond
	electionTimeoutMax   = 500 * time.Millisecond
	electionPollInterval = 20 * time.Millisecond
)

// LogEntry is one replicated log slot: a client command tagged
// with the term the leader appended it in.
type LogEntry struct {
	Command interface{}
	Term    int
}

// serverRole is the role a Raft peer holds in the current term.
type serverRole int

// The three roles; follower is the zero value, so a peer starts
// in that role before its ticker runs.
const (
	follower serverRole = iota
	candidate
	leader
)

// Raft holds one replica's state; mu guards every field except
// applyCh, which only the applier goroutine sends on.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	currentTerm int
	votedFor    int
	log         []LogEntry

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	snapshotPending   bool

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role             serverRole
	electionDeadline time.Time

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// Make constructs a Raft peer, restores any persisted state, and
// starts its ticker, heartbeat, and applier goroutines.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		votedFor:  -1,
		log:       []LogEntry{{Term: 0}},
		applyCh:   applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.resetElectionDeadline()

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.applier()

	return rf
}

// GetState reports the current term and whether this peer is leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// Start appends command to the log if this peer is leader and
// returns the index it will occupy, without waiting for commit.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
	rf.persist()
	rf.broadcastAppendEntries()
	return rf.lastLogIndex(), rf.currentTerm, true
}

// Snapshot discards log entries up to index, which the service
// has already checkpointed into the given snapshot bytes.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}
	// log[0] becomes a sentinel that holds only the term at
	// index; everything before index is discarded.
	trimmedLog := make([]LogEntry, 0, rf.lastLogIndex()-index+1)
	trimmedLog = append(trimmedLog, LogEntry{Term: rf.termAt(index)})
	trimmedLog = append(trimmedLog, rf.log[rf.sliceIndex(index)+1:]...)
	rf.lastIncludedTerm = rf.termAt(index)
	rf.lastIncludedIndex = index
	rf.log = trimmedLog
	rf.snapshot = snapshot
	rf.persist()
}

// PersistBytes reports the persisted Raft state size in bytes;
// the service compares it to its snapshot threshold.
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// ticker fires an election when no heartbeat has reset the
// election deadline in time; runs for the life of the peer.
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		if rf.role != leader && time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(electionPollInterval)
	}
}

// heartbeatLoop sends AppendEntries to all peers on a fixed
// interval whenever this peer is leader.
func (rf *Raft) heartbeatLoop() {
	for {
		rf.mu.Lock()
		if rf.role == leader {
			rf.broadcastAppendEntries()
		}
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}
}

// applier delivers snapshots and committed entries to applyCh
// in log order, releasing mu around every channel send.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for !rf.snapshotPending && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		if rf.snapshotPending {
			message := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.snapshotPending = false
			rf.mu.Unlock()
			rf.applyCh <- message
			rf.mu.Lock()
			continue
		}

		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
			continue
		}

		rf.lastApplied++
		message := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.sliceIndex(rf.lastApplied)].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- message
		rf.mu.Lock()
	}
}

// RequestVoteArgs is the RequestVote RPC request.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote handles a candidate's vote request; grants a vote
// only if the candidate's log is at least as up to date.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	lastIndex := rf.lastLogIndex()
	lastTerm := rf.termAt(lastIndex)
	candidateUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionDeadline()
	}
}

// sendRequestVote issues the RPC and reports whether it was
// delivered; false means no information about the peer's state.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// startElection bumps the term, votes for self, and asks every
// peer for a vote in parallel; the caller holds mu.
func (rf *Raft) startElection() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionDeadline()

	electionTerm := rf.currentTerm
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.termAt(lastIndex)
	votesGranted := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         electionTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role != candidate || rf.currentTerm != electionTerm || !reply.VoteGranted {
				return
			}
			votesGranted++
			if votesGranted == len(rf.peers)/2+1 {
				rf.becomeLeader()
			}
		}(peer)
	}
}

// becomeLeader resets nextIndex and matchIndex and sends a first
// round of AppendEntries; the caller holds mu.
func (rf *Raft) becomeLeader() {
	rf.role = leader
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.lastLogIndex() + 1
		rf.matchIndex[peer] = 0
	}
	rf.broadcastAppendEntries()
}

// AppendEntriesArgs is the AppendEntries RPC request, doubling
// as the heartbeat when Entries is empty.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the AppendEntries RPC response, carrying
// conflict info the leader uses to backtrack nextIndex.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries handles a leader's replication or heartbeat
// request, rejecting stale terms and reporting log conflicts.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.role = follower
	rf.resetElectionDeadline()

	// Skip entries the snapshot already covers and anchor the
	// consistency check at the snapshot boundary.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		skip := rf.lastIncludedIndex - args.PrevLogIndex
		if skip > len(args.Entries) {
			reply.Success = true
			return
		}
		args.Entries = args.Entries[skip:]
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	// On a term mismatch, walk back to that term's first index
	// so the leader can skip it in one round trip.
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		firstIndexOfTerm := args.PrevLogIndex
		for firstIndexOfTerm-1 > rf.lastIncludedIndex && rf.termAt(firstIndexOfTerm-1) == reply.ConflictTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIndex = firstIndexOfTerm
		return
	}

	// Truncate only at a real term conflict, so a delayed RPC
	// cannot erase entries a later one appended.
	for offset, entry := range args.Entries {
		position := args.PrevLogIndex + 1 + offset
		if position <= rf.lastLogIndex() {
			if rf.termAt(position) != entry.Term {
				rf.log = append(rf.log[:rf.sliceIndex(position)], entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		rf.applyCond.Signal()
	}
	reply.Success = true
}

// broadcastAppendEntries replicates to every other peer without
// waiting for replies; the caller holds mu.
func (rf *Raft) broadcastAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.replicateToPeer(peer)
	}
}

// replicateToPeer sends the log suffix after nextIndex, or a
// snapshot if that suffix was compacted; the caller holds mu.
func (rf *Raft) replicateToPeer(peer int) {
	if rf.nextIndex[peer] <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		go func() {
			reply := &InstallSnapshotReply{}
			if !rf.sendInstallSnapshot(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role != leader || rf.currentTerm != args.Term {
				return
			}
			rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
		}()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.termAt(prevLogIndex),
		Entries:      append([]LogEntry{}, rf.log[rf.sliceIndex(prevLogIndex)+1:]...),
		LeaderCommit: rf.commitIndex,
	}
	go func() {
		reply := &AppendEntriesReply{}
		if !rf.peers[peer].Call("Raft.AppendEntries", args, reply) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.role != leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatchIndex
				rf.nextIndex[peer] = newMatchIndex + 1
				rf.advanceCommitIndex()
			}
			return
		}

		// Ignore a reject for an outdated nextIndex.
		if rf.nextIndex[peer] != args.PrevLogIndex+1 {
			return
		}
		rf.nextIndex[peer] = rf.backtrackedNextIndex(reply)
		rf.replicateToPeer(peer)
	}()
}

// backtrackedNextIndex picks the next index to try after a
// rejected AppendEntries, using the follower's conflict term.
func (rf *Raft) backtrackedNextIndex(reply *AppendEntriesReply) int {
	// Scan back for the newest entry in the conflict term; if
	// none remain, fall back to the reported index.
	if reply.ConflictTerm != -1 {
		for sliceIndex := len(rf.log) - 1; sliceIndex >= 1; sliceIndex-- {
			if rf.log[sliceIndex].Term == reply.ConflictTerm {
				return rf.lastIncludedIndex + sliceIndex + 1
			}
		}
	}
	return max(reply.ConflictIndex, 1)
}

// InstallSnapshotArgs is the InstallSnapshot RPC request.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is the InstallSnapshot RPC response.
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot installs a leader's snapshot, replacing any log
// entries it covers and fast-forwarding commit and apply.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.role = follower
	rf.resetElectionDeadline()

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	var newLog []LogEntry
	if args.LastIncludedIndex < rf.lastLogIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog = make([]LogEntry, 0, rf.lastLogIndex()-args.LastIncludedIndex+1)
		newLog = append(newLog, LogEntry{Term: args.LastIncludedTerm})
		newLog = append(newLog, rf.log[rf.sliceIndex(args.LastIncludedIndex)+1:]...)
	} else {
		newLog = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.snapshotPending = true
	rf.persist()
	rf.applyCond.Signal()
}

// sendInstallSnapshot issues the RPC and reports whether it was
// delivered.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// advanceCommitIndex commits the highest current-term index a
// majority has stored; the caller holds mu.
func (rf *Raft) advanceCommitIndex() {
	matched := make([]int, len(rf.peers))
	copy(matched, rf.matchIndex)
	matched[rf.me] = rf.lastLogIndex()
	sort.Ints(matched)
	// After the ascending sort, this slot is the highest index
	// that a majority of peers have reached.
	majorityMatchIndex := matched[(len(matched)-1)/2]

	// Committing only entries from this term avoids exposing a
	// lower-term entry a future leader could still overwrite.
	if majorityMatchIndex > rf.commitIndex && majorityMatchIndex > rf.lastIncludedIndex && rf.termAt(majorityMatchIndex) == rf.currentTerm {
		rf.commitIndex = majorityMatchIndex
		rf.applyCond.Signal()
	}
}

// becomeFollower adopts a higher term and clears the vote so
// the peer can vote in it; the caller holds mu.
func (rf *Raft) becomeFollower(term int) {
	rf.role = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// resetElectionDeadline picks a randomized deadline so peers
// don't all start elections at the same instant.
func (rf *Raft) resetElectionDeadline() {
	timeoutRange := int64(electionTimeoutMax - electionTimeoutMin)
	timeout := electionTimeoutMin + time.Duration(rand.Int63n(timeoutRange))
	rf.electionDeadline = time.Now().Add(timeout)
}

// lastLogIndex returns the highest index present in the log,
// counting entries already compacted into the snapshot.
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// sliceIndex converts a global log index into an offset into log,
// where log[0] is a sentinel holding lastIncludedIndex's term.
func (rf *Raft) sliceIndex(absoluteIndex int) int {
	return absoluteIndex - rf.lastIncludedIndex
}

// termAt returns the term of the entry at a global log index.
func (rf *Raft) termAt(absoluteIndex int) int {
	return rf.log[rf.sliceIndex(absoluteIndex)].Term
}

// persist saves the term, vote, snapshot boundary and log with
// the current snapshot; the caller holds mu.
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)
	encoder.Encode(rf.log)
	rf.persister.Save(buffer.Bytes(), rf.snapshot)
}

// readPersist restores what persist saved and starts commit and
// apply at the snapshot boundary; a decode failure is fatal.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var logEntries []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil ||
		decoder.Decode(&logEntries) != nil {
		log.Fatalf("readPersist: decode failed")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.log = logEntries
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}
