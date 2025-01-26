package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
//import "fmt"
import "sync"
import "time"

type Coordinator struct {
	mu sync.Mutex
	mapTasks []MapTask
	reduceTasks []ReduceTask
	nReduce int
	nMap int
	phase string
	mapTasksCompleted int
	reduceTasksCompleted int
}

type MapTask struct {
	taskNumber int
	filePath string
	status int
	startTime time.Time
}

type ReduceTask struct {
	taskNumber int
	status int
	startTime time.Time
 }

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTasksCompleted < len(c.mapTasks) { // Map phase
		for i := range c.mapTasks {
			if c.mapTasks[i].status == 0 {
				reply.TaskNumber = c.mapTasks[i].taskNumber
				reply.FilePath = c.mapTasks[i].filePath
				reply.Phase = "map"
				reply.HasTask = true
				reply.NReduce = c.nReduce
				c.mapTasks[i].status = 1
				c.mapTasks[i].startTime = time.Now()
				return nil
			}
		}
	} else if c.reduceTasksCompleted < c.nReduce { 
        for i := range c.reduceTasks {
            if c.reduceTasks[i].status == 0 {
                reply.TaskNumber = i
                reply.Phase = "reduce"
                reply.HasTask = true
                reply.NReduce = c.nReduce
				reply.NMap = c.nMap
                c.reduceTasks[i].status = 1
				c.reduceTasks[i].startTime = time.Now()
                return nil
            }
        }
	} else {
			reply.HasTask = false  // No more tasks
	}
	return nil
}

func (c* Coordinator) NotifyComplete(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	//fmt.Printf("%s task completed: %d\n", args.Phase, args.TaskNumber)
	if args.Phase == "map" {
		c.mapTasks[args.TaskNumber].status = 2
		c.mapTasksCompleted += 1
	} else {
		c.reduceTasks[args.TaskNumber].status = 2
		c.reduceTasksCompleted += 1
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.checkTaskTimeouts()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
    defer c.mu.Unlock()
	return c.mapTasksCompleted == len(c.mapTasks) && c.reduceTasksCompleted == c.nReduce
}

func (c *Coordinator) checkTaskTimeouts() {
    for {
        time.Sleep(10 * time.Second)
        c.mu.Lock()
        now := time.Now()
        
        for i := range c.mapTasks {
            if c.mapTasks[i].status == 1 && now.Sub(c.mapTasks[i].startTime) > 10*time.Second {
                c.mapTasks[i].status = 0
            }
        }
        
        for i := range c.reduceTasks {
            if c.reduceTasks[i].status == 1 && now.Sub(c.reduceTasks[i].startTime) > 10*time.Second {
                c.reduceTasks[i].status = 0
            }
        }
        c.mu.Unlock()
    }
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks: make([]MapTask, len(files)),
		reduceTasks: make([]ReduceTask, nReduce),
		nReduce: nReduce,
		nMap: len(files),
		phase: "map",
		mapTasksCompleted: 0,
	}

	for i := range files {
		c.mapTasks[i] = MapTask{
			taskNumber: i,
			filePath: files[i],
			status: 0,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			taskNumber: i,
			status: 0,
		}
	}

	c.server()
	return &c
}
