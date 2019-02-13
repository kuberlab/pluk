package main

import (
	"fmt"
	"runtime"
)

type Version struct {
	version    string
	goCompiler string
}

var VersionStr = "2.2.0"

func (v Version) String() string {
	return fmt.Sprintf("%v", v.version)
}

func GetVersion() Version {
	return Version{
		version:    VersionStr,
		goCompiler: runtime.Version(),
	}
}
