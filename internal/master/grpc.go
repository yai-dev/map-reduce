package master

import (
	context "context"
	"errors"

	"google.golang.org/protobuf/types/known/anypb"
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
		return g.handleHeartbeatOperation(ctx, request)
	case NotifyOp_OP_TASK_COMPLETION: // Task completion operation
	case NotifyOp_OP_ACQUIRE_TASK: // Acquire task operation
	case NotifyOp_OP_REGISTER: // Worker register operation
		return g.handleRegisterOperation(ctx, request)
	case NotifyOp_OP_UNREGISTER: // Worker unregister operation
		return g.handleUnregisterOperation(ctx, request)
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

func (g *GrpcService) handleUnregisterOperation(ctx context.Context, request *NotifyRequest) (*NotifyResponse, error) {
	payload := new(UnregisterPayload)

	if err := request.Payload.UnmarshalTo(payload); err != nil {
		return &NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	if err := g.m.unregister(payload.Identifier); err != nil {
		return &NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	return &NotifyResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}, nil
}

func (g *GrpcService) handleHeartbeatOperation(ctx context.Context, request *NotifyRequest) (*NotifyResponse, error) {
	payload := new(WorkerReportPayload)

	if err := request.Payload.UnmarshalTo(payload); err != nil {
		return &NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	if err := g.m.ping(payload); err != nil {
		return &NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	return &NotifyResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}, nil
}

func (g *GrpcService) handleRegisterOperation(ctx context.Context, request *NotifyRequest) (*NotifyResponse, error) {
	var err error
	var id string
	var resp *NotifyResponse
	var respPayload *RegisteredPayload
	var any *anypb.Any

	payload := new(RegisterPayload)

	if err = request.Payload.UnmarshalTo(payload); err != nil {
		goto RespondFailed
	}

	id = g.m.register(payload)
	resp = &NotifyResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}
	respPayload = &RegisteredPayload{Identifier: id}
	any, err = anypb.New(respPayload)
	if err != nil {
		goto RespondFailed
	}
	resp.Payload = any

	return resp, nil

RespondFailed:
	return &NotifyResponse{
		Succeed:   false,
		Timestamp: timestamppb.Now(),
		Message:   err.Error(),
	}, err
}
