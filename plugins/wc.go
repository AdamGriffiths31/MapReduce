package main

import (
	"mr/common"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []common.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []common.KeyValue{}
	for _, w := range words {
		kv := common.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
