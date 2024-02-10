package distributed

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
)

type RPCArgs struct {
	Name string
}

type RPCReply struct {
	Ok bool
}

type DoJobArgs struct {
	IsMap bool
	File  string
	Name  string
}

type DoJobReply struct {
	Ok bool
}

func coordinatorSock() string {
	s := "/var/tmp/100-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func coordinatorSockName(s string) string {
	return "/var/tmp/100-mr-" + s
}

func call(rpcName string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer c.Close()
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}
	return false
}

func callByName(srv string, rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSockName(srv)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true
}
