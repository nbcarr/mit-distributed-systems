package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskRequest struct {
	
}

type TaskReply struct {
	TaskNumber int
	FilePath string
	Phase string
	HasTask bool
	NReduce int
	NMap int
}

type CompleteTaskArgs struct {
	TaskNumber int
	Phase string
}

type CompleteTaskReply struct {

}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
