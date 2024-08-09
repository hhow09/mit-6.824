package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Tasks between worker and coordinator
type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

type ReportTaskArgs struct {
	Done     bool
	Seq      int
	Phase    TaskPhase
	WorkerId int
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
