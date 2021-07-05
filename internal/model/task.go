package model

import "github.com/suenchunyu/map-reduce/internal/master"

type TaskFlag uint8

const (
	FlagUnknown TaskFlag = iota
	FlagMap
	FlagReduce
)

func FlagMapFromProtoBufferType(typ master.TaskType) TaskFlag {
	switch typ {
	case master.TaskType_TASK_TYPE_REDUCE:
		return FlagReduce
	case master.TaskType_TASK_TYPE_MAP:
		return FlagMap
	default:
		return FlagUnknown
	}
}

type Task struct {
	Object     string
	Flag       TaskFlag
	Finished   bool
	FinishedAt int64
}
