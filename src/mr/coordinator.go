package mr

import "C"
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
	coordinatorType int
	nReduceNum      int
	mapChan         chan string
	reduceChan      chan int
	mp              map[string]int
	mapingMap       map[string]struct{}
	reduceMap       map[int]struct{}
	mu              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.coordinatorType == mapping && len(c.mapChan) != 0 {
		fmt.Println("send mapping task")

		fileName := <-c.mapChan
		reply.WorkerId = c.mp[fileName]
		reply.TaskType = mapping
		reply.TaskName = fileName
		reply.NReduceNum = c.nReduceNum
		reply.TimeStamp = 0
		c.mapingMap[fileName] = struct{}{}

		go func() {
			time.Sleep(5 * time.Second)

			c.mu.Lock()
			defer c.mu.Unlock()

			_, ok := c.mapingMap[fileName]
			if ok {
				delete(c.mapingMap, fileName)
				c.mapChan <- fileName
				fmt.Println(fmt.Sprintf("%s back to mapchan", fileName))
			}
		}()
	} else if c.coordinatorType == reducing && len(c.reduceChan) != 0 {
		fmt.Println("send reducing task")
		index := <-c.reduceChan

		reply.WorkerId = index
		reply.TaskType = reducing
		reply.TimeStamp = 0
		c.reduceMap[index] = struct{}{}

		go func() {
			time.Sleep(10 * time.Second)

			// perevnt race conditions
			c.mu.Lock()
			defer c.mu.Unlock()

			_, ok := c.reduceMap[index]
			if ok {
				delete(c.reduceMap, index)
				c.reduceChan <- index
				fmt.Println(fmt.Sprintf("%d back to reduce chan", index))
			}
		}()
	} else {
		fmt.Println("send waiting task")
		reply.TaskType = waiting
	}
	fmt.Printf("%v\n", reply)
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
	return c.coordinatorType == done
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println(fmt.Sprintf("receive task done\n %v", args))
	if args.TaskType == mapping {
		fmt.Println(fmt.Sprintf("delect maping %s", args.FileName))
		if _, ok := c.mapingMap[args.FileName]; ok {
			delete(c.mapingMap, args.FileName)
			if len(c.mapingMap) == 0 && len(c.mapChan) == 0 {
				c.coordinatorType = reducing
				for i := 0; i < c.nReduceNum; i++ {
					c.reduceChan <- i
				}
			}
		} else {
			fmt.Println(fmt.Sprintf("no mapping %d", args.WorkerId))
		}

	} else if args.TaskType == reducing {
		fmt.Println(fmt.Sprintf("delect reduce %d", args.WorkerId))
		if _, ok := c.reduceMap[args.WorkerId]; ok {
			delete(c.reduceMap, args.WorkerId)
			if len(c.reduceMap) == 0 && len(c.reduceChan) == 0 {
				c.coordinatorType = done
			}
		} else {
			fmt.Println(fmt.Sprintf("no reducing task %d", args.WorkerId))
		}
	}

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	tmpChan := make(chan string, len(files))
	tmpMp := make(map[string]int)
	for i := range files {
		tmpChan <- files[i]
		tmpMp[files[i]] = i
	}
	c := Coordinator{
		coordinatorType: mapping,
		nReduceNum:      nReduce,
		mapChan:         tmpChan,
		reduceChan:      make(chan int, nReduce),
		mapingMap:       make(map[string]struct{}),
		reduceMap:       make(map[int]struct{}),
		mp:              tmpMp,
	}

	c.server()
	return &c
}
