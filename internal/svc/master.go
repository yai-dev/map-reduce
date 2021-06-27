package svc

import "sync"

type Master struct {
    mutex *sync.RWMutex

    nReduce int
    tasks   []*Task
    maps    map[*Worker]*Task
    reduces map[*Worker]*Task
    workers []*Worker
}
