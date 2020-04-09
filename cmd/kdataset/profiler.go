package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Profiler struct {
	lock    sync.RWMutex
	timeMap map[string]time.Duration
}

func NewProfiler() *Profiler {
	return &Profiler{timeMap: make(map[string]time.Duration)}
}

func (p *Profiler) AddTime(name string, t time.Duration) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if _, ok := p.timeMap[name]; ok {
		p.timeMap[name] += t
	} else {
		p.timeMap[name] = t
	}
}

func (p *Profiler) String() string {
	s := strings.Builder{}
	s.WriteString("Profiler:\n")
	for k, v := range p.timeMap {
		s.WriteString(fmt.Sprintf("%v = %v\n", k, v))
	}
	return s.String()
}
