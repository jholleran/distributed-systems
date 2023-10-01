package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	for {
		reply := CallRequestTask()

		if reply.Phase == "map" {
			handleMapTask(mapf, &reply)
		} else {
			handleReduceTask(reducef, &reply)
		}
		// Tell coordinator we are finished the task
		CallTaskCompleted(&reply)
	}
}

func handleMapTask(mapf func(string, string) []KeyValue, mapTask *Task) {

	filename := mapTask.File

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	//log.Printf("Map KVA: %v", kva)

	buckets := make(map[int][]KeyValue)
	for _, value := range kva {
		bucketNumber := ihash(value.Key) % mapTask.NReduce
		//log.Printf("Bucket %v, value: %v", bucketNumber, value)
		buckets[bucketNumber] = append(buckets[bucketNumber], value)
	}

	mapTask.IntermediateFiles = make(map[int]string)

	//log.Printf("Buckets %v", buckets)
	for k, v := range buckets {
		//log.Printf("Bucket number: %v, values: %v", k, v)

		intermediateFilename := fmt.Sprintf("mr-%d-%d", mapTask.Id, k)
		mapTask.IntermediateFiles[k] = intermediateFilename

		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create temp intermediate file")
		}

		enc := json.NewEncoder(intermediateFile)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to temp intermediate file")
			}
		}
	}

	log.Printf("Intermediate files created successfully for: %v\n", filename)

}

func handleReduceTask(reducef func(string, []string) string, reduceTask *Task) {

	log.Printf("Reducing: %v\n", reduceTask)

	intermediate := []KeyValue{}
	for _, filename := range reduceTask.IntermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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

	//log.Printf("Map KVA: %v", kva)

	sort.Sort(ByKey(intermediate))

	reduceTask.File = fmt.Sprintf("mr-out-%d", reduceTask.Id)
	ofile, _ := os.Create(reduceTask.File)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

		log.Printf("Reduce Key: %v: Values: %v", intermediate[i].Key, values)

		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallRequestTask() Task {

	// declare an argument structure.
	args := TaskRequest{}

	// fill in the argument(s).
	//args.X = 99

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			fmt.Printf("reply %v\n", reply)

			if reply.Status == "process" {
				break
			}
			log.Println("Empty task. Nothing to do here.")
		}
		fmt.Printf("request task call failed! Waiting and will try again...\n")
		time.Sleep(time.Second * 2)
	}

	return reply
}

func CallTaskCompleted(task *Task) {

	reply := Task{}

	ok := call("Coordinator.TaskComplete", &task, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
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
