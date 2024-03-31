package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
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
			if reply.TaskType == mapping {
				err = DoMap(mapf, reply)
			} else if reply.TaskType == reducing {
				err = DoReduce(reducef, reply)
			} else if reply.TaskType == waiting {
				time.Sleep(1 * time.Second)
			} else if reply.TaskType == done {
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
	fmt.Println("start map")
	fmt.Println(fmt.Sprintf("%v", reply))
	file, err := os.Open(reply.TaskName)
	if err != nil {
		return fmt.Errorf("file: %s open fail", reply.TaskName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("readall %s file fail", reply.TaskName)
	}

	if err := file.Close(); err != nil {
		return err
	}

	kvgroup := mapf(reply.TaskName, string(content))

	fileGroup := make([]*os.File, reply.NReduceNum)
	encoderGroup := make([]*json.Encoder, reply.NReduceNum)
	for i := range fileGroup {
		tmpFileName := fmt.Sprintf("mr-tmp-%d-%d", reply.WorkerId, i)
		fileGroup[i], err = os.CreateTemp("./", tmpFileName)
		if err != nil {
			return err
		}
		encoderGroup[i] = json.NewEncoder(fileGroup[i])
	}

	//sort.Slice(kvgroup, func(i, j int) bool {
	//	return kvgroup[i].Key < kvgroup[j].Key
	//})

	indexGroup := make([][]KeyValue, reply.NReduceNum)
	for _, kv := range kvgroup {
		hashIndex := ihash(kv.Key) % reply.NReduceNum
		indexGroup[hashIndex] = append(indexGroup[hashIndex], kv)
	}

	for i, indexKv := range indexGroup {
		if err := encoderGroup[i].Encode(indexKv); err != nil {
			return err
		}
	}

	for i, f := range fileGroup {
		if err := f.Close(); err != nil {
			return fmt.Errorf("file mr-tmp-%d-%d close file %s", reply.WorkerId, i, err)
		}
	}

	for i := range fileGroup {
		newFileName := fmt.Sprintf("./out-mr-%d-%d", reply.WorkerId, i)
		oldFileNmae := fileGroup[i].Name()
		if err := os.Rename(oldFileNmae, newFileName); err != nil {
			return fmt.Errorf("change name false, old name %s, new name %s", oldFileNmae, newFileName)
		} else {
			fmt.Println(fmt.Sprintf("change name success, old name: %s, new name: %s", oldFileNmae, newFileName))
		}
	}

	return CallDone(&Args{WorkerId: reply.WorkerId, TimeStamp: reply.TimeStamp, TaskType: reply.TaskType, FileName: reply.TaskName}, &Reply{})
}

func DoReduce(reducef func(string, []string) string, reply *Reply) error {
	fileDir, err := os.ReadDir("./")
	if err != nil {
		return err
	}
	likeName := fmt.Sprintf("out-mr-.*-%s$", strconv.Itoa(reply.WorkerId))
	kvGroup := make([]KeyValue, 0)
	for i := range fileDir {
		if !fileDir[i].IsDir() {
			regex := regexp.MustCompile(likeName)
			if regex.MatchString(fileDir[i].Name()) {
				fmt.Println(fmt.Sprintf("now filename is %s, like name is %s", fileDir[i].Name(), likeName))
				tmpFileName := fmt.Sprintf("./%s", fileDir[i].Name())
				fmt.Println(tmpFileName)
				tmpFile, err := os.Open(tmpFileName)
				if err != nil {
					return fmt.Errorf("open file: %s fail", tmpFileName)
				}
				defer tmpFile.Close()

				nowDecoder := json.NewDecoder(tmpFile)
				var tmpKvGroup []KeyValue
				err = nowDecoder.Decode(&tmpKvGroup)
				if err != nil {
					return fmt.Errorf(fmt.Sprintf("decoder file %s, %s", fileDir[i].Name(), err))
				}
				kvGroup = append(kvGroup, tmpKvGroup...)
			}
		}
	}
	fmt.Println("fmt.print kvgroup")
	fmt.Println(kvGroup)
	sort.Slice(kvGroup, func(i, j int) bool {
		return kvGroup[i].Key < kvGroup[j].Key
	})
	//fmt.Printf("%v\n", kvGroup)
	//time.Sleep(2 * time.Second)

	outPutFile, err := os.CreateTemp("./", fmt.Sprintf("mr-out-tmp-%d", reply.WorkerId))
	if err != nil {
		return nil
	}

	i := 0
	for i < len(kvGroup) {
		j := i + 1
		for j < len(kvGroup) && kvGroup[i].Key == kvGroup[j].Key {
			j++
		}
		value := []string{}
		for k := i; k < j; k++ {
			value = append(value, kvGroup[k].Value)
		}
		output := reducef(kvGroup[i].Key, value)
		_, err = fmt.Fprintf(outPutFile, "%v %v\n", kvGroup[i].Key, output)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("write file false %v", err))
		}

		i = j
	}

	oldName := fmt.Sprintf(outPutFile.Name())
	newName := fmt.Sprintf("mr-out-%d", reply.WorkerId)
	err = os.Rename(oldName, newName)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("change name false %v", err))
	} else {
		fmt.Println(fmt.Sprintf("change name succuse old name: %s new name: %s", oldName, newName))
	}

	return CallDone(&Args{TaskType: reply.TaskType, WorkerId: reply.WorkerId, FileName: reply.TaskName}, &Reply{})
}

func CallDone(args *Args, reply *Reply) error {
	if err := call("Coordinator.TaskDone", args, reply); err == false {
		return fmt.Errorf("call Done false, filename %s, timestamp int %d", "", 0)
	}
	return nil
}

func CallTAsk() (*Reply, bool) {
	args := Args{TimeStamp: 1000}
	reply := Reply{}
	if ok := call("Coordinator.AskTask", &args, &reply); ok {
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
