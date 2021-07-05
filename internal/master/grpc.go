package master

import (
	context "context"
	"errors"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrUnknownOperation = errors.New("unknown operation")
)

type GrpcService struct {
	m *Master
}

func (g *GrpcService) Notify(ctx context.Context, request *NotifyRequest) (*NotifyResponse, error) {
	switch request.Op {
	case NotifyOp_OP_HEARTBEAT: // Heartbeat operation
	case NotifyOp_OP_TASK_COMPLETION: // Task completion operation
	case NotifyOp_OP_ACQUIRE_TASK: // Acquire task operation
	case NotifyOp_OP_REGISTER: // Worker register operation
	case NotifyOp_OP_UNREGISTER: // Worker unregister operation
	case NotifyOp_OP_UNKNOWN: // Unknown operation
		goto UnknownOperation
	default:
		goto UnknownOperation
	}

UnknownOperation:
	return &NotifyResponse{
		Succeed:   false,
		Timestamp: timestamppb.Now(),
		Message:   ErrUnknownOperation.Error(),
	}, ErrUnknownOperation
}
