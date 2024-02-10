package main

import (
	"log"
	"mr/common"
	"mr/distributed"
	"os"
	"plugin"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Worker missing argument got ", len(os.Args))
	}
	log.Println("Worker: Starting")
	mapf, reducef := loadPlugin(os.Args[1])
	distributed.RunWorker(mapf, reducef)
}

func loadPlugin(filename string) (func(string, string) []common.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v\nerror: %v", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []common.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
