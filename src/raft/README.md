# Raft Lab 
## Test
```
./test.sh
```
- test 10 times to ensure consistently corret.

## Lab1 Notes
- `GetState()` need to be accessed by config, therefore `state`, `currentTerm` are accessed with atomic to avoid using lock.
- we should keep **election timeout** randomnize in certain range to avoid split vote.
- always check the state before communicate with peers (request vote, respond, heartbeat...)
- always check the term when receiving request, response from peer
    - if node's own term is outdated, become follower
- `sendAppendEntries` with 0 log entry ==> means heartbeat 
