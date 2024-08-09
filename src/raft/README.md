# Raft Lab
- Link: http://nil.csail.mit.edu/6.824/2021/labs/lab-raft.html
- Paper: http://nil.csail.mit.edu/6.824/2021/papers/raft-extended.pdf

## Run Test
```
./test.sh
```
- test 10 times to ensure consistently corret.

## 2A: leader election
- Pull Request & test result: https://github.com/hhow09/mit-6.824/pull/1

### Task
```
Implement Raft leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run go test -run 2A -race to test your 2A code. 
```

### Overview
- implement raft state machine
- implement leader election
    - follower timeout
    - candidate request vote and collect vote
    - candidate becomes leader
    - leader heartbeat

### Notes
- `GetState()` need to be accessed by config, therefore `state`, `currentTerm` are accessed with atomic to avoid using lock.
- we should keep **election timeout** randomnize in certain range to avoid split vote.
- always check the state before communicate with peers (request vote, respond, heartbeat...)
- always check the term when receiving request, response from peer
    - if node's own term is outdated, become follower
- `sendAppendEntries` with 0 log entry ==> means heartbeat 

## 2B: log
- Pull Request & test result: https://github.com/hhow09/mit-6.824/pull/2

### Task
```
Implement the leader and follower code to append new log entries, so that the go test -run 2B -race tests pass. 
```

### Overview
- implement leader replicating logs to 
    - update `matchIndex` and `nextIndex` when request success
- implement logs consistency check when follower receives `AppendEntries`
- implement committing logs (tracking with `matchIndex`)
- implement applying messages to state machine when logs are considered committed
- implement the election restriction
- implement Leader Completeness Property

### Notes
- `nextIndex` is "optmistic" records of peers' log index.
    - could decrease when peer reject the append log request.
- `matchIndex` is "pessimistic" records of peers' log index.
    - in normal case, `matchIndex[i] + 1 = nextIndex[i]`
    - `commitIndex` should based on `matchIndex` (`(rf *Raft) commit(nodeID int, term int)`)

### [TestRejoin2B](./test_test.go)
initial State
```
A: [{nil},{101, 1}]
B: [{nil},{101, 1}]
C: [{nil},{101, 1}]
```

- A as leader1
- A disconnected
```
A: [{nil},{101, 1}, {102, 1},{103, 1},{104, 1}]
B: [{nil},{101, 1}]
C: [{nil},{101, 1}]
```

- B elected as leader2
```
A: [{nil},{101, 1}, {102, 1},{103, 1},{104, 1}]
B: [{nil},{101, 1},{103, 2}]
C: [{nil},{101, 1},{103, 2}]
```
- B disconnected
- A (leader1) is back (as leader state)
- A Start agreement 104
    - C rgject the AppendEntries since A has lower term

- C timeout, election => C win as leader 3
- C Start agreement 104
    - `cfg.one(104, 2, true)` has retry
- C update A's logs with `{104, 3}`

#### Expected Final State
```
A: [{nil},{101, 1},{103, 2}, {104, 3}]
B: [{nil},{101, 1},{103, 2}]
C: [{nil},{101, 1},{103, 2}, {104, 3}]
```
- Uncommited logs `{102, 1},{103, 1},{104, 1}` in A are removed.

## 2C: persistence
- Pull Request & test result: https://github.com/hhow09/mit-6.824/pull/5

### Task
```
Complete the functions persist() and readPersist() in raft.go by adding code to save and restore persistent state. You will need to encode (or "serialize") the state as an array of bytes in order to pass it to the Persister. Use the labgob encoder; see the comments in persist() and readPersist(). labgob is like Go's gob encoder but prints error messages if you try to encode structures with lower-case field names. 
```

### Overview
- implement state encoding and persistence based on the `Persistent state` in paper Fig. 2.
- implement optimization of conflicting entry retry in at the bottom of paper page 7

### Notes
- `persist()` before responding to RPCs: `Start()`, `AppendEntries()`, `RequestVote()` (Fig. 2)
- if RPC fails, simply let it retry at netxt heartbeat, no need to write retry logic.
- For debug, print out the logs, request reply to fix the boundary condition
- The lock strategy here is:
    - lock -> read state -> prepare request arguments -> unlock 
    - send request
    - lock -> read result -> update state -> unlock

## 2D: log compaction
- Pull Request & test result: https://github.com/hhow09/mit-6.824/pull/5

### Task
```
Modify your Raft code to support snapshots: implement Snapshot, CondInstallSnapshot, and the InstallSnapshot RPC, as well as the changes to Raft to support these (e.g, continue to operate with a trimmed log). Your solution is complete when it passes the 2D tests and all the Lab 2 tests. (Note that lab 3 will test snapshots more thoroughly than lab 2 because lab 3 has a real service to stress Raft's snapshots.) 
```

### Overview
- implement snapshot
- implement sending snapshot to follower
- implement log trimming when follower receiving snapshot.

### Flows
#### Snapshot
1. Application calls `Snapshot` to raft node.
    - ref: `applierSnap` of [config.go](./config.go)
2. Raft `Snapshot`

#### Sync Snapshot
3. Leader send `InstallSnapshot` RPC to follower
4. Follower received `InstallSnapshot` RPC from leader
    - follower checks the snapshot freshness
    - if ok, send `ApplyMsg` to application through channel.
5. Application received `ApplyMsg` and calls `CondInstallSnapshot` to install snapshot.