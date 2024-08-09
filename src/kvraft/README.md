# Lab KVRaft
- Lab and requirement: http://nil.csail.mit.edu/6.824/2021/labs/lab-kvraft.html
- paper: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf

## Part A: Key/value service without snapshots
- Pull Request & test result: https://github.com/hhow09/mit-6.824/pull/8

### Task 1
```
Your first task is to implement a solution that works when there are no dropped messages, and no failed servers.

You'll need to add RPC-sending code to the Clerk Put/Append/Get methods in client.go, and implement PutAppend() and Get() RPC handlers in server.go. These handlers should enter an Op in the Raft log using Start(); you should fill in the Op struct definition in server.go so that it describes a Put/Append/Get operation. Each server should execute Op commands as Raft commits them, i.e. as they appear on the applyCh. An RPC handler should notice when Raft commits its Op, and then reply to the RPC.

You have completed this task when you reliably pass the first test in the test suite: "One client".
```

### Task 2
```
Add code to handle failures, and to cope with duplicate Clerk requests, including situations where the Clerk sends a request to a kvserver leader in one term, times out waiting for a reply, and re-sends the request to a new leader in another term. The request should execute just once. Your code should pass the go test -run 3A -race tests. 
```

### Architecture
- 1 kv server - (1 raft node + 1 state machine)
- 1 client (Clerk) can send to many kv server

### Notes
- client does not know who is leader before sending request.
- client needs client ID and request ID for server to de-duplicate requests in unreliable network.
    - server keep track of last operation of a client.
- [raft](../raft) code are slightly updated for performance.
- For each request, server `Start()` then waits for raft applying message through `applyCh`.
    - multiple command could wait concurrently
