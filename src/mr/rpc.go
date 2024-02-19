package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type Task struct {
	TID int
	*MapTask
	*ReduceTask
}

type MapTask struct {
	FileName string
	R        int
}

type ReduceTask struct {
	Ri    int
	Paths []string
}

type TaskArgs struct {
	ID int
}

type TaskReply struct {
	Task
}

type MapTaskDoneArgs struct {
	ID    int
	Paths []string
}

type MapTaskDoneReply struct{}

type ReduceTaskDoneArgs struct {
	ID   int
	Path string
}

type ReduceTaskDoneReply struct{}

type AskForIntermediate struct {
	Ri int
}

type ReplyForIntermediate struct {
	Paths []string
	EOF   bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
