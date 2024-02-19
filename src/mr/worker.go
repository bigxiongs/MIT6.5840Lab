package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	// acquire a unique identifier
	id := os.Getpid()
	// an infinite loop asking for a task
	for i := 0; ; i++ {
		task := GetTask(id)
		if task.MapTask != nil {
			paths := DoMap(*task.MapTask, mapf, i)
			MapTaskDone(id, paths)
		} else if task.ReduceTask != nil {
			path := DoReduce(*task.ReduceTask, reducef, i)
			ReduceTaskDone(id, path)
		} else {
			log.Printf("worker %v closing\n", id)
			break
		}
	}

}

// GetTask gets a task from coordinator
func GetTask(id int) Task {
	args := TaskArgs{ID: id}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		//log.Printf("worker %v acquire task %v\n", id, reply.Task)
	} else {
		//log.Println("no task assigned")
	}
	return reply.Task
}

func MapTaskDone(id int, paths []string) {
	args := MapTaskDoneArgs{id, paths}
	reply := MapTaskDoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if ok {
		//log.Println("map task done")
	} else {
		log.Println("task done but couldn't notice coordinator")
	}
}

func ReduceTaskDone(id int, path string) {
	args := ReduceTaskDoneArgs{id, path}
	reply := ReduceTaskDoneReply{}
	ok := call("Coordinator.ReduceTaskDone", &args, &reply)
	if ok {
		//log.Println("reduce task done")
	} else {
		log.Println("task done but couldn't notice coordinator")
	}
}

func DoMap(task MapTask, mapf func(string, string) []KeyValue, tid int) []string {
	pid := os.Getpid()
	filename := task.FileName
	paths := make([]string, task.R)
	buffers := make([]*os.File, task.R)
	// open all temporal file
	for i := 0; i < task.R; i++ {
		// specific tmp file for the process and the task
		buffers[i], _ = os.CreateTemp("", strconv.Itoa(pid)+"-"+strconv.Itoa(tid)+"-"+strconv.Itoa(i))
		paths[i] = buffers[i].Name()
		defer buffers[i].Close()
	}

	// reading input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	for _, kv := range kva {
		k := ihash(kv.Key) % task.R

		_, err := fmt.Fprintf(buffers[k], "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}

	return paths
}

func DoReduce(task ReduceTask, reducef func(string, []string) string, tid int) string {
	pid := os.Getpid()
	paths := task.Paths
	intermediate := make([]KeyValue, 0)
	for _, path := range paths {
		file, _ := os.Open(path)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			kv := strings.Split(line, " ")
			intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	ofile, _ := os.CreateTemp("", strconv.Itoa(pid)+"-"+strconv.Itoa(tid)+"-out")
	defer ofile.Close()
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return ofile.Name()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
