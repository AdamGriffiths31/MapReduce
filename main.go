package main

import (
	"fmt"
	"io/ioutil"
	"mr/common"
	"mr/distributed"
	"time"
)

func main() {
	fmt.Println("Map Reduce")
	co := distributed.MakeCoordinator(loadDocuments())
	for co.Done() == false {
		time.Sleep(1 * time.Second)
	}
}

func loadDocuments() []common.Task {
	docs := []common.Task{}

	docs = append(docs, loadDocument("pg-being_ernest.txt"))
	docs = append(docs, loadDocument("pg-dorian_gray.txt"))
	docs = append(docs, loadDocument("pg-frankenstein.txt"))
	docs = append(docs, loadDocument("pg-grimm.txt"))
	docs = append(docs, loadDocument("pg-huckleberry_finn.txt"))
	docs = append(docs, loadDocument("pg-metamorphosis.txt"))
	docs = append(docs, loadDocument("pg-sherlock_holmes.txt"))
	docs = append(docs, loadDocument("pg-tom_sawyer.txt"))

	return docs
}

func loadDocument(name string) common.Task {
	fileContents, err := ioutil.ReadFile("data/" + name)
	if err != nil {
		fmt.Println("Error reading file ", name)
		panic(err)
	}
	return common.Task{TaskName: name, TaskData: string(fileContents)}
}
