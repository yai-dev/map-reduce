package model

import (
	"github.com/suenchunyu/map-reduce/internal/pkg/master"
)

type TaskFlag uint8

func (t TaskFlag) String() string {
	switch t {
	case FlagMap:
		return "Map"
	case FlagReduce:
		return "Reduce"
	default:
		return "Unknown"
	}
}

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
	ID         string
	Object     string
	Flag       TaskFlag
	Finished   bool
	FinishedAt int64
}
