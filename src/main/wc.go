package main

import (
	"os"
	"strconv"
	"strings"
)
import "fmt"
import "mapreduce"
import "container/list"
import "unicode"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	l := list.New()
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	wordArray := strings.FieldsFunc(value, f)
	for _, v := range wordArray {
		l.PushBack(mapreduce.KeyValue{Key: v, Value: "1"})
	}
	return l
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	var sum = 0
	for e := values.Front(); e != nil; e = e.Next() {
		n, _ := strconv.Atoi(e.Value.(string))
		sum += n
	}
	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	fmt.Println("Reached Main")
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			fmt.Println("Reached sequential")
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			fmt.Println("Reached Non sequential")
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
