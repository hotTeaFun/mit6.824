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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"log"
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
	CommandTerm  int
	// For 2D:
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
	ElectionTimeout = 150 * time.Millisecond
	HeatBeatTimeout = 100 * time.Millisecond
)

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
	role              RaftRole
	currentTerm       int
	votedFor          int
	log               []*LogEntry
	lastIncludedTerm  int
	lastIncludedIndex int
	snapshot          []byte

	nPeers      int
	commitIndex int
	lastApplied int
	leaderId    int
	nextIndex   []int
	matchIndex  []int

	applyCond *sync.Cond
	applyChan chan ApplyMsg

	lastActiveTime  time.Time
	timeoutInterval time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm

	isleader = rf.role == RaftLeader
	return term, isleader
}

func (rf *Raft) lastLogTermAndLastLogIndex() (int, int) {
	logIndex := rf.lastLogIndex()
	logTerm := rf.log[logIndex-rf.lastIncludedIndex].Term
	return logTerm, logIndex
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}
func (rf *Raft) logTerm(logIndex int) int {
	return rf.log[logIndex].Term
}

func (rf *Raft) logEntry(logIndex int) *LogEntry {
	if logIndex > rf.lastLogIndex() {
		return rf.log[0]
	}
	logIndex = logIndex - rf.lastIncludedIndex
	if logIndex <= 0 {
		return rf.log[0]
	}
	return rf.log[logIndex]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.leaderId = leaderId
		rf.log = log
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.lastIncludedTerm = rf.log[0].Term
		rf.lastIncludedIndex = lastIncludedIndex
	}
}

type InstallSnapshotArgs struct {
	Term             int //leader's term
	LeaderId         int
	LastIncludeIndex int //snapshot中最后一条日志的index
	LastIncludeTerm  int
	Data             []byte //快照
	//Offset           int //此次传输chunk在快照文件的偏移量，快照文件可能很大，因此需要分chunk，此次不分片
	//Done             bool //是否最后一块
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term || args.Data == nil {
		log.Default().Printf("Term:[%d],Peer:[%d],Leader:[%d],InstallSnapshot term invalid or data nil\n",
			rf.currentTerm, rf.me, rf.leaderId)
		return
	}
	if rf.currentTerm < args.Term {
		log.Default().Printf("Term:[%d],Peer:[%d],Leader:[%d],InstallSnapshot bigger Term[%d] found\n",
			rf.currentTerm, rf.me, rf.leaderId, args.Term)
		rf.role = RaftFollower
		rf.currentTerm = args.Term
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	rf.lastActiveTime = time.Now()
	if rf.commitIndex >= args.LastIncludeIndex || args.LastIncludeIndex <= rf.lastIncludedIndex {
		return
	}
	log := rf.log[0:1]
	log[0].Term = args.LastIncludeTerm
	//本结点最后一条日志在快照点之前，太落后，清空，应用快照，否则截断
	if rf.lastLogIndex() > args.LastIncludeIndex {
		log = append(log, rf.log[args.LastIncludeIndex-rf.lastIncludedIndex+1:]...)
	}
	rf.log = log
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludeTerm
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}

	rf.persist()
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
	go func() {
		rf.applyChan <- applyMsg
	}()
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
	//lastApplied是已经应用到状态机的最后一条日志，也是压缩点。
	if index <= rf.lastIncludedIndex || index != rf.lastApplied || index > rf.lastLogIndex() {
		return
	}
	logs := rf.log[0:1]
	logs[0].Term = rf.log[index-rf.lastIncludedIndex].Term
	logs = append(logs, rf.log[index-rf.lastIncludedIndex+1:]...)
	rf.log = logs
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = logs[0].Term
	rf.persister.Save(rf.encodeRaftState(), snapshot)
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
	Term         int
	VoterGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoterGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = RaftFollower
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.persist()
	}
	if rf.votedFor == RoleNone || rf.votedFor == args.CandidateId {
		lastTerm, lastIndex := rf.lastLogTermAndLastLogIndex()
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

type AppendEntriesArgs struct {
	Term                int
	LeaderId            int
	PrevLogTerm         int
	PrevLogIndex        int
	LeaderCommitedIndex int
	Entries             []*LogEntry
}

// 心跳或者日志追加
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
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = RoleNone
		rf.leaderId = RoleNone
		rf.role = RaftFollower
		rf.persist()
	}
	rf.votedFor = args.LeaderId
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.NextIndex = rf.lastLogIndex() + 1
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.NextIndex = rf.lastIncludedIndex + 1
		reply.Success = false
		return
	}
	if args.PrevLogTerm != rf.logTerm(args.PrevLogIndex-rf.lastIncludedIndex) {
		//前一条日志的任期不匹配，找到冲突term首次出现的地方
		index := args.PrevLogIndex - rf.lastIncludedIndex
		term := rf.logTerm(index)
		for ; index > 0 && rf.logTerm(index) == term; index-- {
		}
		reply.NextIndex = index + 1 + rf.lastIncludedIndex
		return
	}
	if args.PrevLogIndex < rf.lastLogIndex() {
		rf.log = rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex]
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommitedIndex > rf.commitIndex {
		commitIndex := args.LeaderCommitedIndex
		//结点可能很落后
		if rf.lastLogIndex() < commitIndex {
			commitIndex = rf.lastLogIndex()
		}
		rf.commitIndex = commitIndex
	}
	rf.persist()
	reply.Success = true
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	reply.NextIndex = rf.nextIndex[rf.me]
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)
		var applyMsg *ApplyMsg
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.lastApplied >= rf.commitIndex {
				return
			}
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
			}
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
	// Your code here (2B).

	return index, term, true
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
func randElectionTimeout() time.Duration {
	electionTimeout := rand.Intn(150) + 250 // [250,400)
	return time.Duration(electionTimeout * int(time.Millisecond))
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 1)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role == RaftLeader {
				return
			}
			if time.Since(rf.lastActiveTime) < rf.timeoutInterval {
				return
			}
			if rf.role == RaftFollower {
				rf.role = RaftCandidate
			}
			rf.lastActiveTime = time.Now()
			rf.timeoutInterval = randElectionTimeout()
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()
			lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
			rf.mu.Unlock()
			replyTerm, voteGranted := rf.gatherVotes(lastLogTerm, lastLogIndex)
			rf.mu.Lock()
			if rf.role != RaftCandidate {
				return
			}
			if replyTerm > rf.currentTerm {
				rf.role = RaftFollower
				rf.currentTerm = replyTerm
				rf.votedFor = RoleNone
				rf.leaderId = RoleNone
				rf.persist()

			} else if voteGranted > rf.nPeers/2 {
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
			if rf.role != RaftLeader {
				return
			}
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
						if !ok {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.currentTerm != args.Term || rf.role != RaftLeader {
							return
						}
						//发现更大的term，本结点是旧leader
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = RoleNone
							rf.leaderId = RoleNone
							rf.role = RaftFollower
							rf.persist()
							return
						}
						rf.matchIndex[server] = rf.lastIncludedIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						matchIndice := make([]int, rf.nPeers)
						copy(matchIndice, rf.matchIndex)
						sort.Slice(matchIndice, func(i, j int) bool {
							return matchIndice[i] < matchIndice[j]
						})
						newCommitIndex := matchIndice[rf.nPeers/2]
						if newCommitIndex > rf.commitIndex && rf.logTerm(newCommitIndex-rf.lastIncludedIndex) == rf.currentTerm {
							if newCommitIndex > rf.lastLogIndex() {
								rf.commitIndex = rf.lastLogIndex()
							} else {
								rf.commitIndex = newCommitIndex
							}
							log.Default().Printf("InstallSnap Term:[%d] Leader:[%d] new Commit Index:%d\n", rf.currentTerm, rf.leaderId, rf.commitIndex)
						}

					}(i, argsI)
				} else {
					prevLogIndex := rf.matchIndex[i]
					if lastIndex := rf.lastLogIndex(); prevLogIndex > lastIndex {
						prevLogIndex = lastIndex
					}
					args := &AppendEntriesArgs{
						Term:                rf.currentTerm,
						LeaderId:            rf.me,
						PrevLogIndex:        prevLogIndex,
						PrevLogTerm:         rf.logTerm(prevLogIndex - rf.lastIncludedIndex),
						LeaderCommitedIndex: rf.commitIndex,
					}
					if rf.matchIndex[i] < rf.lastLogIndex() {
						args.Entries = make([]*LogEntry, 0)
						args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]...)
					}
					go func(server int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, args, reply)
						if !ok {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.currentTerm != args.Term {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = RoleNone
							rf.leaderId = RoleNone
							rf.role = RaftFollower
							rf.persist()
							return
						}
						rf.nextIndex[server] = reply.NextIndex
						rf.matchIndex[server] = reply.NextIndex - 1
						if reply.Success {
							matchIndice := make([]int, rf.nPeers)
							copy(matchIndice, rf.matchIndex)
							sort.Slice(matchIndice, func(i, j int) bool {
								return matchIndice[i] < matchIndice[j]
							})
							newCommitIndex := matchIndice[rf.nPeers/2]
							//只能往大更新
							if newCommitIndex > rf.commitIndex && rf.logTerm(newCommitIndex-rf.lastIncludedIndex) == rf.currentTerm {
								//如果commitIndex比自己实际的日志长度还大，这时需要减小
								if newCommitIndex > rf.lastLogIndex() {
									rf.commitIndex = rf.lastLogIndex()
								} else {
									rf.commitIndex = newCommitIndex
								}
								log.Default().Printf("AppendEntries Term:[%d] Leader:[%d] new Commit Index:%d\n", rf.currentTerm, rf.me, rf.commitIndex)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.nPeers = len(peers)
	rf.lastActiveTime = time.Now()
	rf.timeoutInterval = randElectionTimeout()
	rf.votedFor = RoleNone
	rf.leaderId = RoleNone
	rf.role = RaftFollower
	rf.currentTerm = None
	rf.log = make([]*LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.log = append(rf.log, &LogEntry{
		Term: 0,
	})
	rf.commitIndex = None
	rf.lastApplied = None
	rf.applyChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapshot = persister.ReadSnapshot()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()
	go rf.applyLogLoop(applyCh)
	return rf
}
