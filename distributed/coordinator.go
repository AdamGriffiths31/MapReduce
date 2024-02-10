package distributed

import (
	"fmt"
	"io/ioutil"
	"log"
	"mr/common"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Coordinator struct {
	sync.Mutex
	workers chan string
	tasks   []common.Task
	done    bool
}

func MakeCoordinator(input []common.Task) *Coordinator {
	log.Printf("Coordinator: %v", len(input))
	c := Coordinator{tasks: input, workers: make(chan string, 10)}
	c.server()
	c.schedule(true)
	c.loadReduceTasks()
	c.schedule(false)
	c.writeResult()
	c.done = true
	return &c
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	socketName := coordinatorSock()
	os.Remove(socketName)

	l, e := net.Listen("unix", socketName)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

func (c *Coordinator) Register(args RPCArgs, reply *RPCReply) error {
	c.Lock()
	defer c.Unlock()

	c.workers <- args.Name

	reply.Ok = true
	return nil
}

func (c *Coordinator) Done() bool {
	return c.done
}

func (c *Coordinator) schedule(isMap bool) {
	log.Printf("Coordinator: schedule")
	if c.tasks == nil || len(c.tasks) == 0 {
		log.Printf("Coordinator: schedule: no tasks")
		return
	}
	completed := 0
	jobs := len(c.tasks)

	for completed < jobs {
		if len(c.workers) == 0 {
			continue
		}
		if len(c.tasks) == 0 {
			continue
		}
		worker := <-c.workers
		task := c.tasks[0]
		c.tasks = c.tasks[1:]
		go func(w string, t common.Task) {
			status := callByName(w, "Worker.DoJob", DoJobArgs{IsMap: isMap, File: t.TaskData, Name: t.TaskName}, &DoJobReply{})
			if !status {
				fmt.Printf("Failed to schedule task %v to worker %v\n", t.TaskName, w)
				c.tasks = append(c.tasks, t)
			} else {
				completed++
				c.workers <- w
			}
		}(worker, task)
	}
}

func (c *Coordinator) loadReduceTasks() {
	log.Printf("Coordinator: loadReduceTasks")
	reduceTasks := []common.Task{}

	files, err := ioutil.ReadDir("tmp")
	if err != nil {
		fmt.Println("Error reading directory")
		log.Fatal(err)
	}

	for _, file := range files {
		reduceTasks = append(reduceTasks, common.Task{TaskName: file.Name()})
	}

	c.tasks = reduceTasks
}

func (c *Coordinator) writeResult() {
	log.Printf("Coordinator: writeResult")
	result := make(map[string]int)
	files, err := ioutil.ReadDir("./tmpOut")
	if err != nil {
		fmt.Println("Error reading directory")
		log.Fatal(err)
	}

	for _, file := range files {
		contents, err := os.ReadFile("tmpOut/" + file.Name())
		if err != nil {
			fmt.Println("Error reading file")
			log.Fatal(err)
		}

		lines := strings.Split(string(contents), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.Split(line, " ")
			k, v := kv[0], kv[1]
			intV, _ := strconv.Atoi(v)
			result[k] += intV
		}
	}

	outName := "mr-out"
	var output []byte
	for k, v := range result {
		output = append(output, fmt.Sprintf("%v %v\n", k, v)...)
	}
	os.WriteFile(outName, output, 0644)
}
