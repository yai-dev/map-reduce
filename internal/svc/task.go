package svc

type TaskFlag uint8

const (
    Map TaskFlag = iota
    Reduce
)

type Task struct {
    file     string
    flag     TaskFlag
    finished bool
    assigned *Worker
}
