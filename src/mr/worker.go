package mr

import (
	"encoding/json"
	"fmt"
	"io"
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
	// CallExample()

	for {
		if reply, ok := CallTAsk(); ok != false {
			var err error
			if reply.taskType == mapType {
				err = DoMap(mapf, reply)
			} else if reply.taskType == reduceType {
				DoReduce(reducef, reply.taskName)
			} else if reply.taskType == Waitting {
				time.Sleep(1 * time.Second)
			} else if reply.taskType == done {
				break
			}

			if err != nil {
				fmt.Println(err)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func DoMap(mapf func(string, string) []KeyValue, reply *Reply) error {

	file, err := os.Open(reply.taskName)
	if err != nil {
		return fmt.Errorf("file: %s open fail", reply.taskName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("readall %s file fail", reply.taskName)
	}

	if err := file.Close(); err != nil {
		return err
	}

	kvgroup := mapf(reply.taskName, string(content))

	fileGroup := make([]*os.File, reply.nReduceNum)
	encoderGroup := make([]*json.Encoder, reply.nReduceNum)
	for i := range fileGroup {
		fileGroup[i], err = os.CreateTemp("mroutput", fmt.Sprintf("mr-tmp-%d-%d", reply.workerId, i))
		encoderGroup[i] = json.NewEncoder(fileGroup[i])
	}

	sort.Slice(kvgroup, func(i, j int) bool {
		return kvgroup[i].Key < kvgroup[j].Key
	})

	for _, kv := range kvgroup {
		hashIndex := ihash(kv.Key)
		if err := encoderGroup[hashIndex].Encode(kv); err != nil {
			return err
		}
	}

	for i, f := range fileGroup {
		if err := f.Close(); err != nil {
			return fmt.Errorf("file mr-tmp-%d-%d close file", reply.workerId, i, err)
		}
	}

	for i := range fileGroup {
		newFileName := fmt.Sprintf("mr-%d-%d", reply.workerId, i)
		oldFileNmae := fmt.Sprintf("mr-tmp-%d-%d", reply.workerId, i)
		if err := os.Rename(oldFileNmae, newFileName); err != nil {
			return fmt.Errorf("change name false, old name %s, new name %s", oldFileNmae, newFileName)
		}
	}

	return CallDone(&Args{fileName: reply.taskName, timeStamp: reply.timeStamp, taskType: reply.taskType}, nil)
}

func DoReduce(reducef func(string, []string) string, reduceNum int) error {
	for i := 0; i < reduceNum; i++ {
		fileDir, err := os.ReadDir("mroutput")
		if err != nil {
			return err
		}
		for i := range fileDir {
			if fileDir[i].
		}
	}
}

func CallDone(args *Args, reply *Reply) error {
	if err := call("Coordinator.Done", args, reply); err == false {
		return fmt.Errorf("call Done false, filename %s, timestamp int %d", "", 0)
	}
	return nil
}

func CallTAsk() (*Reply, bool) {
	args := Args{}
	reply := Reply{}
	if ok := call("Corrdinator.AskTask", &args, &reply); ok {
		return &reply, true
	} else {
		return nil, false
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
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
