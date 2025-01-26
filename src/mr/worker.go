package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "os"
import "io/ioutil"
import "sort"

 // For sorting
 type ByKey []KeyValue
 func (a ByKey) Len() int           { return len(a) }
 func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
 func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := RequestTask()
		if !reply.HasTask {
			// No more tasks, exit worker
			return
		}
		//fmt.Printf("Received %s task (%d)\n", reply.Phase, reply.TaskNumber)
		switch reply.Phase {
		case "map":
			doMap(mapf, reply.TaskNumber, reply.FilePath, reply.NReduce)
			NotifyComplete(reply.TaskNumber, "map")
		case "reduce":
			doReduce(reducef, reply.TaskNumber, reply.NMap)
			NotifyComplete(reply.TaskNumber, "reduce")
		}
	}
}

func NotifyComplete(taskNumber int, phase string) {
	args := CompleteTaskArgs{TaskNumber: taskNumber, Phase: phase}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.NotifyComplete", &args, &reply)
	if !ok {
		fmt.Printf("Failed to notify completion of task %v\n", taskNumber)
	}
 }

 func doReduce(reducef func(string, []string) string, taskNumber int, nMap int) {
	// Read all intermediate files for this reduce task
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskNumber)
		file, err := os.Open(filename)
		if err != nil {
			continue // Skip if file doesn't exist
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
 
	// Sort by key
	sort.Sort(ByKey(intermediate))
 
	// Process each key group
	oname := fmt.Sprintf("mr-out-%d", taskNumber)
	ofile, _ := os.Create(oname)
 
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
 }
 
func doMap(mapf func(string, string) []KeyValue, taskNumber int, filePath string, nReduce int) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	file.Close()
	
	kva := mapf(filePath, string(content))
	
	// Write intermediate KV pairs to files
	for _, kv := range kva {
		// Get correct reducer for this key using ihash
		reducer := ihash(kv.Key) % nReduce
		
		// Create/open intermediate file
		filename := fmt.Sprintf("mr-%d-%d", taskNumber, reducer)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}
		
		// Write KV pair
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
		file.Close()
	}
 }

func RequestTask() TaskReply {
	args := TaskRequest{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Printf("Failed to contact coordinator\n")
		time.Sleep(time.Second) // Back off before retrying
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
