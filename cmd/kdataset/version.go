package main

import (
	"fmt"
	"runtime"
)

type Version struct {
	version    string
	goCompiler string
}

func (v Version) String() string {
	return fmt.Sprintf("%v, Golang %v", v.version, v.goCompiler)
}

func GetVersion() Version {
	return Version{
		version:    "1.0.9",
		goCompiler: runtime.Version(),
	}
}
