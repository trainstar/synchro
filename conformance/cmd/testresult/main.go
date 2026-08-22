package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "suite" {
		os.Exit(runSuite(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "exact" {
		os.Exit(runExact(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "junit" {
		os.Exit(runJUnit(os.Args[2:], os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "jest" {
		os.Exit(runJest(os.Args[2:], os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "xcresult" {
		os.Exit(runXCResult(os.Args[2:], os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "rust" {
		os.Exit(runRust(os.Args[2:], os.Stdout, os.Stderr))
	}

	target := flag.String("test", "", "exact top-level test name")
	flag.Parse()
	if !validTargetName(*target) || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "testresult requires one exact top-level test name")
		os.Exit(2)
	}
	fmt.Println(classifyTestResult(os.Stdin, *target))
}
