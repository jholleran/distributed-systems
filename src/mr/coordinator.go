package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTasks     []Task
	reducedTasks []Task
	phase        string
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskRequest, reply *Task) error {

	// Locking the full request task process. This could be made more efficient.
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Received request for Task...\n")

	if c.phase == "map" {
		for _, mapTask := range c.mapTasks {
			if mapTask.Status == "init" {
				// Found!
				mapTask.Status = "process"
				reply.Id = mapTask.Id
				reply.Phase = mapTask.Phase
				reply.File = mapTask.File
				reply.NReduce = mapTask.NReduce
				reply.Status = mapTask.Status

				go func(task *Task) {
					timer := time.NewTimer(30 * time.Second)
					<-timer.C
					fmt.Println("Task Timer fired")
					if task.Status != "complete" {
						task.Status = "init"
						log.Printf("Task not complete and reset back to init: %v\n", task)
					}
				}(&mapTask)

				log.Printf("Returning Task: %v\n", reply)
				return nil
			}
		}

		for _, mapTask := range c.mapTasks {
			if mapTask.Status != "complete" {
				log.Println("Not all Jobs have been completed. Returning wait task.")
				reply.Status = "wait"
				reply.Phase = mapTask.Phase
				return nil
			}
		}

		log.Println("All Map Tasks have been completed. Returning returning reduce task.")
		for _, reducedTask := range c.reducedTasks {
			for _, mapTask := range c.mapTasks {
				if mapTask.IntermediateFiles[reducedTask.Id] != "" {
					reducedTask.IntermediateFiles[mapTask.Id] = mapTask.IntermediateFiles[reducedTask.Id]
				}
			}
		}

		//for _, reducedTask := range c.reducedTasks {
		//	log.Printf("Reduced task: %v\n", reducedTask.Id)
		//	for k, v := range reducedTask.IntermediateFiles {
		//		fmt.Printf(" - intermediate file. k: %v, v: %v\n", k, v)
		//	}
		//}

		c.phase = "reduce"

	} else if c.phase == "reduce" {

		//log.Printf("Reduced tasks: %v\n", c.reducedTasks)
		for i, reduceTask := range c.reducedTasks {
			if reduceTask.Status == "init" {
				// Found!
				c.reducedTasks[i].Status = "process"
				reduceTask.Status = "process"
				reply.Id = reduceTask.Id
				reply.Phase = reduceTask.Phase
				reply.Status = reduceTask.Status
				reply.IntermediateFiles = reduceTask.IntermediateFiles

				go func(task *Task) {
					timer := time.NewTimer(30 * time.Second)
					<-timer.C
					fmt.Println("Task Timer fired")
					if task.Status != "complete" {
						task.Status = "init"
						log.Printf("Task not complete and reset back to init: %v\n", task)
					}
				}(&c.reducedTasks[i])

				log.Printf("Returning Task: %v\n", reply)
				return nil
			}
		}

		for _, reduceTask := range c.reducedTasks {
			if reduceTask.Status != "complete" {
				log.Println("Not all reduce tasks have been completed. Returning wait task.")
				reply.Status = "wait"
				reply.Phase = reduceTask.Phase
				return nil
			}
		}

		log.Println("All Reduce Tasks have been completed.")
		c.phase = "completed"
		reply.Phase = c.phase
	} else {
		reply.Phase = c.phase
	}

	log.Printf("Returning Task: %v\n", reply)
	return nil
}

func (c *Coordinator) TaskComplete(args *Task, reply *Task) error {

	// Locking the full task complete process. This could be made more efficient.
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Phase == "map" {
		c.mapTasks[args.Id].Status = "complete"
		c.mapTasks[args.Id].IntermediateFiles = args.IntermediateFiles
		reply.Status = c.mapTasks[args.Id].Status
		log.Printf("Map Task id: %d has been comleted\n\t- intermediate files: %v", args.Id, args.IntermediateFiles)
	} else {
		c.reducedTasks[args.Id].Status = "complete"
		reply.Status = c.reducedTasks[args.Id].Status
		log.Printf("Reduce Task id: %d has been comleted\n\t- output file: %v", args.Id, args.File)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Locking the full done process.
	c.mu.Lock()
	defer c.mu.Unlock()

	return "completed" == c.phase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.mapTasks = []Task{}

	for i, f := range files {
		c.mapTasks = append(c.mapTasks, Task{Id: i, Phase: "map", File: f, NReduce: nReduce, Status: "init"})
	}

	log.Printf("Map Tasks created: %v\n", c.mapTasks)

	c.reducedTasks = []Task{}
	for i := 0; i < nReduce; i++ {
		c.reducedTasks = append(c.reducedTasks, Task{Id: i, Phase: "reduce", Status: "init", IntermediateFiles: make(map[int]string)})
	}

	c.phase = "map"

	c.server()
	return &c
}
