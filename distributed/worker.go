package distributed

import (
	"fmt"
	"log"
	"mr/common"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Worker struct {
	mapf    func(string, string) []common.KeyValue
	reducef func(string, []string) string
	Name    string
}

func RunWorker(mapf func(string, string) []common.KeyValue,
	reduceF func(string, []string) string) {
	log.Printf("RunWorker %v\n", os.Getpid())
	name := strconv.Itoa(os.Getpid())
	w := Worker{mapf: mapf, reducef: reduceF, Name: name}
	w.serve()
	ok := call("Coordinator.Register", RPCArgs{Name: w.Name}, &struct{}{})
	if !ok {
		log.Fatal("Register error")
	}
	for {
	}
}

func (w *Worker) DoJob(args *DoJobArgs, reply *DoJobReply) error {
	log.Printf("%q DoJob %q\n", w.Name, args.Name)
	var err error
	if args.IsMap {
		err = w.mapF(args.Name, args.File)
	} else {
		err = w.reduce(args.Name)
	}
	if err != nil {
		log.Printf("DoJob error: %v\n", err)
	}
	w.randomiseBehaviour()
	reply.Ok = err == nil
	return nil
}

func (w *Worker) Heartbeat(args *DoJobArgs, reply *DoJobReply) error {
	reply.Ok = true
	return nil
}

func (w *Worker) serve() {
	rpc.Register(w)
	rpc.HandleHTTP()

	socketName := coordinatorSockName(w.Name)
	log.Printf("Worker: %v\n", socketName)
	os.Remove(socketName)

	l, e := net.Listen("unix", socketName)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

func (w *Worker) writeResultsToFile(out interface{}, name string, directory string) error {
	outName := directory + "/mr-" + name
	var output []byte

	switch data := out.(type) {
	case []common.KeyValue:
		for _, kv := range data {
			output = append(output, fmt.Sprintf("%v %v\n", kv.Key, kv.Value)...)
		}
	case map[string]int:
		for k, v := range data {
			output = append(output, fmt.Sprintf("%v %v\n", k, v)...)
		}
	default:
		return fmt.Errorf("Unknown type: %T", data)
	}

	return os.WriteFile(outName, output, 0644)
}

func (w *Worker) reduce(fileName string) error {
	contents, err := os.ReadFile("tmp/" + fileName)
	if err != nil {
		return err
	}

	result := make(map[string]int)
	lines := strings.Split(string(contents), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}
		lineParts := strings.Split(line, " ")
		keyVal := common.KeyValue{Key: lineParts[0], Value: lineParts[1]}

		reduceResult := w.reducef(keyVal.Key, []string{keyVal.Value})
		intVal, _ := strconv.Atoi(reduceResult)
		result[keyVal.Key] += intVal
	}
	return w.writeResultsToFile(result, fileName, "tmpOut")
}

func (w *Worker) mapF(taskName string, taskData string) error {
	out := w.mapf(taskName, taskData)
	return w.writeResultsToFile(out, taskName, "tmp")
}

func (w *Worker) randomiseBehaviour() {
	rand := time.Now().UnixNano() % 10
	time.Sleep(time.Duration(rand) * time.Second)

	if rand == 1 {
		fmt.Printf("Worker %q: Randomly exiting\n", w.Name)
		os.Exit(1)
	}
}
