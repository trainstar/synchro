package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	target := flag.String("test", "", "exact top-level test name")
	flag.Parse()
	if !validTargetName(*target) || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "testresult requires one exact top-level test name")
		os.Exit(2)
	}
	fmt.Println(classifyTestResult(os.Stdin, *target))
}
