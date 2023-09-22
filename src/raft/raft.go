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
	// "log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type RaftRole int16

const (
	RaftLeader RaftRole = iota
	RaftFollower
	RaftCandidate
)
const RoleNone int = -1
const None int = 0

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
	HeatBeatTimeout    = 50 * time.Millisecond
)

func randElectionTimeout() time.Duration {
	electionTimeout := rand.Int63n(
		MaxElectionTimeout.Milliseconds()-MinElectionTimeout.Milliseconds()) +
		MinElectionTimeout.Milliseconds()
	return time.Duration(electionTimeout) * time.Millisecond
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	nPeers    int
	persister *Persister
	me        int
	dead      int32

	// persistent state on all
	currentTerm       int
	votedFor          int
	leaderId          int
	log               []*LogEntry
	lastIncludedTerm  int
	lastIncludedIndex int

	// volatile state on all
	role        RaftRole
	snapshot    []byte
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex       []int
	matchIndex      []int
	lastActiveTime  time.Time
	timeoutInterval time.Duration

	// applyCond *sync.Cond
	applyChan chan ApplyMsg
}

func (rf *Raft) Persister() *Persister {
	return rf.persister
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RaftLeader
}

func (rf *Raft) lastLogIndex() int {
	// Note: the first log entry is a dummy entry,
	// so it's ok to set rf.lastIncludedIndex to 0 initially.
	// the lastLogIndex must add lastIncludedIndex because it's used to do rpc request
	// which means it's global.
	// and different peers may have different snapshot,
	// so their lastIncludedIndex may be different.
	return len(rf.log) - 1 + rf.lastIncludedIndex
}
func (rf *Raft) logTerm(logIndex int) int {
	return rf.log[logIndex-rf.lastIncludedIndex].Term
}

func (rf *Raft) lastLogTermAndIndex() (int, int) {
	logIndex := rf.lastLogIndex()
	logTerm := rf.logTerm(logIndex)
	return logTerm, logIndex
}

func (rf *Raft) logEntry(logIndex int) *LogEntry {
	// Case1: when logIndex exceed lastLogIndex, we consider the dummy entry as the result
	if logIndex > rf.lastLogIndex() {
		return rf.log[0]
	}
	logIndex = logIndex - rf.lastIncludedIndex
	// Case2: when logIndex is too small, we consider the dummy entry as the result too.
	if logIndex <= 0 {
		return rf.log[0]
	}
	// Case3: otherwise seek for the corresponding entry.
	return rf.log[logIndex]
}

func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeRaftState(), rf.snapshot)
}
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.leaderId)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
	// no persist data
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm       int
		voteFor           int
		leaderId          int
		log               []*LogEntry
		lastIncludedIndex int
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&leaderId) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		fmt.Println("raft.readPersist Decode error")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
	rf.votedFor = voteFor
	rf.leaderId = leaderId
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = rf.log[0].Term
	// when readPersist is called, the peer must restore to the identical state as the snapshot
	// so lastApplied is set to lastIncludedIndex
	// but the commitIndex is also set to lastIncludedIndex
	// beacause commitIndex >= lastApplied
	// so it's safe for commitIndex to started with lastApplied here
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

}

type InstallSnapshotArgs struct {
	Term             int //leader's term
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
	//Offset           int
	//Done             bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// Case1: snapshot from older term or no sanpshot data
	// just ignore
	if rf.currentTerm > args.Term || args.Data == nil {
		//DPrintf("Term:[%d],Peer:[%d],Leader:[%d],InstallSnapshot term invalid or data nil\n",
		//	rf.currentTerm, rf.me, rf.leaderId)
		return
	}
	// Case2: snapshot from bigger term
	// update term and turn to follower
	if rf.currentTerm < args.Term {
		//DPrintf("Term:[%d],Peer:[%d],Leader:[%d],InstallSnapshot bigger Term[%d] found\n",
		//	rf.currentTerm, rf.me, rf.leaderId, args.Term)
		rf.role = RaftFollower
		rf.currentTerm = args.Term
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	// Case3: same term and non-null snapshot data
	// try to apply snapshot

	// Step1: update lastActiveTime
	rf.lastActiveTime = time.Now()
	// Step2: check if need
	// already committed or newer snapshot
	if rf.commitIndex >= args.LastIncludeIndex || args.LastIncludeIndex <= rf.lastIncludedIndex {
		return
	}
	// Step3: clip the log
	log := rf.log[0:1]
	log[0].Term = args.LastIncludeTerm
	if rf.lastLogIndex() > args.LastIncludeIndex {
		log = append(log, rf.log[args.LastIncludeIndex-rf.lastIncludedIndex+1:]...)
	}
	rf.log = log
	// Step4: update state
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludeTerm
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	// Step5: do persist
	rf.persist()
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	// Step6: commit apply msg asynchronously.
	go func() {
		rf.applyChan <- applyMsg
	}()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	// Step3: clip the log
	log := rf.log[0:1]
	log[0].Term = lastIncludedTerm
	if rf.lastLogIndex() > lastIncludedIndex {
		log = append(log, rf.log[lastIncludedIndex-rf.lastIncludedIndex+1:]...)
	}
	rf.log = log
	// Step4: update state
	rf.snapshot = snapshot
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	// Step5: do persist
	rf.persist()
	// DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Step1: check if no need to do snapshot
	// 1. snapshot is old
	// 2. index is not valid
	// 3. index is too high
	if index <= rf.lastIncludedIndex || index != rf.lastApplied || index > rf.lastLogIndex() {
		return
	}
	// Step2: clip the log
	logs := rf.log[0:1]
	logs[0].Term = rf.log[index-rf.lastIncludedIndex].Term
	logs = append(logs, rf.log[index-rf.lastIncludedIndex+1:]...)
	rf.log = logs
	// Step3: update state
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = logs[0].Term
	// Step4: do persist
	rf.persister.Save(rf.encodeRaftState(), snapshot)
}

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
	Term         int
	VoterGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoterGranted = false
	reply.Term = rf.currentTerm
	// Case1: smaller term
	// just ignore
	if args.Term < rf.currentTerm {
		return
	}
	// Case2: bigger term
	// update term and turn to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = RaftFollower
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	// Case3: no voted for or already voted for
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastTerm, lastIndex := rf.lastLogTermAndIndex()
		// check the log term and index
		// vote for rule:
		// 1. bigger log term
		// 2. same log term but not less log index
		if lastTerm < args.LastLogTerm || lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex {
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			rf.role = RaftFollower
			reply.VoterGranted = true
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			rf.persist()
			return
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term                int
	LeaderId            int
	PrevLogTerm         int
	PrevLogIndex        int
	LeaderCommitedIndex int
	Entries             []*LogEntry
}

type AppendEntriesReply struct {
	Success   bool
	Term      int
	NextIndex int
	Msg       string
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// Case1: smaller term
	// just ignore
	if args.Term < rf.currentTerm {
		return
	}
	// Case2: bigger term
	// update term and turn to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.role = RaftFollower
		rf.persist()
	}
	// Note: must overwrite state
	// cannot refuse if conflicted because leader is unique in the same term
	rf.votedFor = args.LeaderId
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
	// Case1: not enough log
	// set NextIndex to the lastLogIndex+1 and return
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.NextIndex = rf.lastLogIndex() + 1
		return
	}
	// Case2: snapshot contains some new log
	// set NextIndex to the lastIncludedIndex+1 and return
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.NextIndex = rf.lastIncludedIndex + 1
		return
	}
	// Case3: prev log term conflict
	// find the last log whoes term is same
	// set NextIndex and return
	if args.PrevLogTerm != rf.logTerm(args.PrevLogIndex) {
		index := args.PrevLogIndex
		term := rf.logTerm(index)
		for ; index > rf.lastIncludedIndex && rf.logTerm(index) == term; index-- {
		}
		reply.NextIndex = index + 1
		return
	}
	// Case4: prev log term ok
	// Step1. clip the log
	if args.PrevLogIndex < rf.lastLogIndex() {
		rf.log = rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex]
	}
	// Step2. append new log entries
	rf.log = append(rf.log, args.Entries...)
	// Step3. update commitIndex if necessary
	if args.LeaderCommitedIndex > rf.commitIndex {
		commitIndex := args.LeaderCommitedIndex
		if rf.lastLogIndex() < commitIndex {
			commitIndex = rf.lastLogIndex()
		}
		rf.commitIndex = commitIndex
	}
	// Step4. do persist
	rf.persist()
	// Step5. update private matchIndex and nextIndex
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	// Step6. set reply
	reply.Success = true
	reply.NextIndex = rf.nextIndex[rf.me]
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)
		var applyMsg *ApplyMsg = nil
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// check if no need
			if rf.lastApplied >= rf.commitIndex {
				return
			}
			// update lastApplied if too old
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
			}
			// check if there is any new entry committed
			if rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				applyMsg = &ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntry(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.logEntry(rf.lastApplied).Term,
				}
			}
		}()
		// apply log asynchronously
		if applyMsg != nil {
			go func() {
				applyCh <- *applyMsg
			}()
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RaftLeader {
		return -1, -1, false
	}
	entry := &LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index = rf.lastLogIndex()
	term = rf.currentTerm
	rf.persist()

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 1)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// if already leader, just return
			if rf.role == RaftLeader {
				return
			}
			// check if timeout
			if time.Since(rf.lastActiveTime) < rf.timeoutInterval {
				return
			}
			// turn from follwer to candidate
			if rf.role == RaftFollower {
				rf.role = RaftCandidate
			}
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()
			lastLogTerm, lastLogIndex := rf.lastLogTermAndIndex()
			// Unlock here to gatherVotes
			rf.mu.Unlock()
			replyTerm, voteGranted := rf.gatherVotes(lastLogTerm, lastLogIndex)
			rf.mu.Lock()
			// After gatherVotes check result
			// Case1: no longer candidate any more
			// just return
			if rf.role != RaftCandidate {
				return
			}
			// Case2: bigger reply term
			// update term and turn to follower
			if replyTerm > rf.currentTerm {
				rf.role = RaftFollower
				rf.currentTerm = replyTerm
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
				rf.persist()
				return
			}
			// Case3: enough votes
			// turn to leader
			if voteGranted > rf.nPeers/2 {
				rf.leaderId = rf.me
				rf.role = RaftLeader
				rf.lastActiveTime = time.Unix(0, 0)
				rf.persist()
			}
		}()
	}
}
func (rf *Raft) gatherVotes(lastTerm, lastIndex int) (int, int) {
	var (
		nPeers      int
		me          int
		currentTerm int
	)
	rf.mu.Lock()
	nPeers = rf.nPeers
	me = rf.me
	currentTerm = rf.currentTerm
	rf.mu.Unlock()
	type RequestVoteResult struct {
		peerId int
		resp   *RequestVoteReply
	}
	voteCh := make(chan *RequestVoteResult, nPeers-1)
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  me,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}
	for i := 0; i < nPeers; i++ {
		if i == me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			resp := reply
			if !ok {
				resp = nil
			}
			voteCh <- &RequestVoteResult{
				peerId: server,
				resp:   resp,
			}
		}(i, args)
	}
	replyTerm := currentTerm
	voteGranted := 1
	totVotes := 1
	for i := 0; i < nPeers-1; i++ {
		vote := <-voteCh
		totVotes++
		if vote.resp != nil {
			if vote.resp.VoterGranted {
				voteGranted++
			}
			if vote.resp.Term > replyTerm {
				replyTerm = vote.resp.Term
			}
		}
		// if enough vote granted or all responded, return
		if voteGranted > nPeers/2 || totVotes == nPeers {
			return replyTerm, voteGranted
		}
	}
	return replyTerm, voteGranted
}

func (rf *Raft) heartBeatLoop() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 20)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// only leader can do broadcast heartbead
			if rf.role != RaftLeader {
				return
			}
			// check if timeout
			if time.Since(rf.lastActiveTime) < HeatBeatTimeout {
				return
			}
			rf.lastActiveTime = time.Now()
			rf.matchIndex[rf.me] = rf.lastLogIndex()
			rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
			for i := 0; i < rf.nPeers; i++ {
				if rf.me == i {
					continue
				}
				// check if snapshot should be sent
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					argsI := &InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.lastIncludedIndex,
						LastIncludeTerm:  rf.lastIncludedTerm,
						Data:             rf.snapshot,
					}
					go func(server int, args *InstallSnapshotArgs) {
						reply := &InstallSnapshotReply{}
						ok := rf.sendInstallSnapshot(server, args, reply)
						// now check result

						// Case1: fail
						if !ok {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						// Case2: term confict or not leader now
						if rf.currentTerm != args.Term || rf.role != RaftLeader {
							return
						}
						// Case3: bigger term
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = RoleNone
							rf.leaderId = RoleNone
							rf.role = RaftFollower
							rf.persist()
							return
						}
						// Case4: success

						// Step1: update matchIndex and nextIndex
						rf.matchIndex[server] = rf.lastIncludedIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						// Step2: update CommitIndex if ok
						matchIndice := make([]int, rf.nPeers)
						copy(matchIndice, rf.matchIndex)
						sort.Slice(matchIndice, func(i, j int) bool {
							return matchIndice[i] < matchIndice[j]
						})
						newCommitIndex := matchIndice[rf.nPeers/2]
						if newCommitIndex > rf.commitIndex && rf.logTerm(newCommitIndex) == rf.currentTerm {
							if newCommitIndex > rf.lastLogIndex() {
								rf.commitIndex = rf.lastLogIndex()
							} else {
								rf.commitIndex = newCommitIndex
							}
							//DPrintf("InstallSnap Term:[%d] Leader:[%d] new Commit Index:%d\n", rf.currentTerm, rf.leaderId, rf.commitIndex)
						}

					}(i, argsI)
				} else {
					// Send normal log entries

					// Step1: calc prevLogIndex
					prevLogIndex := rf.matchIndex[i]
					if lastIndex := rf.lastLogIndex(); prevLogIndex > lastIndex {
						prevLogIndex = lastIndex
					}
					args := &AppendEntriesArgs{
						Term:                rf.currentTerm,
						LeaderId:            rf.me,
						PrevLogIndex:        prevLogIndex,
						PrevLogTerm:         rf.logTerm(prevLogIndex),
						LeaderCommitedIndex: rf.commitIndex,
					}
					// Step2: calc args.Entries
					if rf.matchIndex[i] < rf.lastLogIndex() {
						args.Entries = make([]*LogEntry, 0)
						args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]...)
					}
					go func(server int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, args, reply)
						// now check result

						// Case1: fail
						if !ok {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						// Case2: term conflict
						if rf.currentTerm != args.Term {
							return
						}
						// Case3: found bigger term
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = RoleNone
							rf.leaderId = RoleNone
							rf.role = RaftFollower
							rf.persist()
							return
						}
						// Case4: success

						// Step1: update nextIndex and matchIndex
						rf.nextIndex[server] = reply.NextIndex
						rf.matchIndex[server] = reply.NextIndex - 1
						// Step2: update commitIndex if ok
						if reply.Success {
							matchIndice := make([]int, rf.nPeers)
							copy(matchIndice, rf.matchIndex)
							sort.Slice(matchIndice, func(i, j int) bool {
								return matchIndice[i] < matchIndice[j]
							})
							newCommitIndex := matchIndice[rf.nPeers/2]
							if newCommitIndex > rf.commitIndex && rf.logTerm(newCommitIndex) == rf.currentTerm {
								if newCommitIndex > rf.lastLogIndex() {
									rf.commitIndex = rf.lastLogIndex()
								} else {
									rf.commitIndex = newCommitIndex
								}
								//DPrintf("AppendEntries Term:[%d] Leader:[%d] new Commit Index:%d\n", rf.currentTerm, rf.me, rf.commitIndex)
							}
						}
					}(i, args)
				}
			}
		}()
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
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		mu:              sync.Mutex{},
		nPeers:          len(peers),
		lastActiveTime:  time.Now(),
		timeoutInterval: randElectionTimeout(),
		votedFor:        RoleNone,
		leaderId:        RoleNone,
		role:            RaftFollower,
		currentTerm:     None,
		log:             make([]*LogEntry, 0),
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		commitIndex:     None,
		lastApplied:     None,
		applyChan:       applyCh,
		// applyCond : sync.NewCond(&rf.mu),
		snapshot: persister.ReadSnapshot(),
	}
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.log = append(rf.log, &LogEntry{
		Term: 0,
	})
	rf.readPersist(persister.ReadRaftState())

	go rf.electionLoop()
	go rf.heartBeatLoop()
	go rf.applyLogLoop(applyCh)
	return rf
}
