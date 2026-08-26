// Raft leader election and log replication, Figure 2 of the Raft paper.
package raft

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Election timeout is several heartbeats so a live leader is never mistaken for a dead one.
const (
	heartbeatInterval    = 100 * time.Millisecond
	electionTimeoutMin   = 300 * time.Millisecond
	electionTimeoutMax   = 500 * time.Millisecond
	electionPollInterval = 20 * time.Millisecond
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type serverRole int

const (
	follower serverRole = iota
	candidate
	leader
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role             serverRole
	electionDeadline time.Time

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

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
	// Randomised per peer so split votes resolve quickly.
	rf.resetElectionDeadline()

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.applier()

	return rf
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

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

func (rf *Raft) Snapshot(index int, snapshot []byte) {
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

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

// Leaders send empty AppendEntries at a fixed interval to hold followers' election timers.
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

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		message := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		// Applies outside the lock so a slow consumer never stalls the protocol.
		rf.mu.Unlock()
		rf.applyCh <- message
		rf.mu.Lock()
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

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
	lastTerm := rf.log[lastIndex].Term
	// Section 5.4.1: refuse a candidate whose log is behind ours.
	candidateUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	// One vote per term; a repeat request from the same candidate is granted again for idempotence.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionDeadline()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) startElection() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionDeadline()

	electionTerm := rf.currentTerm
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.log[lastIndex].Term
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

func (rf *Raft) becomeLeader() {
	rf.role = leader
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.lastLogIndex() + 1
		rf.matchIndex[peer] = 0
	}
	rf.broadcastAppendEntries()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

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

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	// Report the conflicting term so the leader can back up a whole term per RPC, not one entry.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		firstIndexOfTerm := args.PrevLogIndex
		for firstIndexOfTerm > 1 && rf.log[firstIndexOfTerm-1].Term == reply.ConflictTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIndex = firstIndexOfTerm
		return
	}

	for offset, entry := range args.Entries {
		position := args.PrevLogIndex + 1 + offset
		if position <= rf.lastLogIndex() {
			if rf.log[position].Term != entry.Term {
				rf.log = append(rf.log[:position], entry)
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

func (rf *Raft) broadcastAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.replicateToPeer(peer)
	}
}

func (rf *Raft) replicateToPeer(peer int) {
	prevLogIndex := rf.nextIndex[peer] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      append([]LogEntry{}, rf.log[prevLogIndex+1:]...),
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

		if rf.nextIndex[peer] != args.PrevLogIndex+1 {
			return
		}
		rf.nextIndex[peer] = rf.backtrackedNextIndex(reply)
		rf.replicateToPeer(peer)
	}()
}

// Fast backup from the follower's conflict hint instead of decrementing nextIndex by one.
func (rf *Raft) backtrackedNextIndex(reply *AppendEntriesReply) int {
	if reply.ConflictTerm != -1 {
		for index := rf.lastLogIndex(); index >= 1; index-- {
			if rf.log[index].Term == reply.ConflictTerm {
				return index + 1
			}
		}
	}
	return max(reply.ConflictIndex, 1)
}

func (rf *Raft) advanceCommitIndex() {
	matched := make([]int, len(rf.peers))
	copy(matched, rf.matchIndex)
	matched[rf.me] = rf.lastLogIndex()
	sort.Ints(matched)
	majorityMatchIndex := matched[(len(matched)-1)/2]

	// Section 5.4.2: only entries from the current term are committed by counting replicas.
	if majorityMatchIndex > rf.commitIndex && rf.log[majorityMatchIndex].Term == rf.currentTerm {
		rf.commitIndex = majorityMatchIndex
		rf.applyCond.Signal()
	}
}

// Any RPC carrying a higher term demotes us immediately.
func (rf *Raft) becomeFollower(term int) {
	rf.role = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) resetElectionDeadline() {
	timeoutRange := int64(electionTimeoutMax - electionTimeoutMin)
	timeout := electionTimeoutMin + time.Duration(rand.Int63n(timeoutRange))
	rf.electionDeadline = time.Now().Add(timeout)
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// Persistence is out of scope here.
func (rf *Raft) persist() {
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
}
