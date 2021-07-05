package model

type TaskFlag uint8

const (
	FlagMap TaskFlag = iota
	FlagReduce
)

type Task struct {
	Object     string
	Flag       TaskFlag
	Finished   bool
	FinishedAt int64
}
