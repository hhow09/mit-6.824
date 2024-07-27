# Raft Lab 
## Test
```
./test.sh
```
- test 10 times to ensure consistently corret.

## Lab2A Notes
- `GetState()` need to be accessed by config, therefore `state`, `currentTerm` are accessed with atomic to avoid using lock.
- we should keep **election timeout** randomnize in certain range to avoid split vote.
- always check the state before communicate with peers (request vote, respond, heartbeat...)
- always check the term when receiving request, response from peer
    - if node's own term is outdated, become follower
- `sendAppendEntries` with 0 log entry ==> means heartbeat 

## Lab2B Notes
- `nextIndex` is "optmistic" records of peers' log index.
    - could decrease when peer reject the append log request.
- `matchIndex` is "conservative" records of peers' log index.
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