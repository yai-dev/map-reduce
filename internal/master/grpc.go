package master

import (
	"context"
	"errors"

	"github.com/suenchunyu/map-reduce/internal/pkg/master"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrUnknownOperation = errors.New("unknown operation")
)

type GrpcService struct {
	m *Master
}

func (g *GrpcService) Notify(ctx context.Context, request *master.NotifyRequest) (*master.NotifyResponse, error) {
	switch request.Op {
	case master.NotifyOp_OP_HEARTBEAT: // Heartbeat operation
		return g.handleHeartbeatOperation(ctx, request)
	case master.NotifyOp_OP_TASK_COMPLETION: // Task completion operation
	case master.NotifyOp_OP_ACQUIRE_TASK: // Acquire task operation
	case master.NotifyOp_OP_REGISTER: // Worker register operation
		return g.handleRegisterOperation(ctx, request)
	case master.NotifyOp_OP_UNREGISTER: // Worker unregister operation
		return g.handleUnregisterOperation(ctx, request)
	case master.NotifyOp_OP_UNKNOWN: // Unknown operation
		goto UnknownOperation
	default:
		goto UnknownOperation
	}

UnknownOperation:
	return &master.NotifyResponse{
		Succeed:   false,
		Timestamp: timestamppb.Now(),
		Message:   ErrUnknownOperation.Error(),
	}, ErrUnknownOperation
}

func (g *GrpcService) handleUnregisterOperation(ctx context.Context, request *master.NotifyRequest) (*master.NotifyResponse, error) {
	payload := new(master.UnregisterPayload)

	if err := request.Payload.UnmarshalTo(payload); err != nil {
		return &master.NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	if err := g.m.unregister(payload.Identifier); err != nil {
		return &master.NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	return &master.NotifyResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}, nil
}

func (g *GrpcService) handleHeartbeatOperation(ctx context.Context, request *master.NotifyRequest) (*master.NotifyResponse, error) {
	payload := new(master.WorkerReportPayload)

	if err := request.Payload.UnmarshalTo(payload); err != nil {
		return &master.NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	if err := g.m.ping(payload); err != nil {
		return &master.NotifyResponse{
			Succeed:   false,
			Timestamp: timestamppb.Now(),
			Message:   err.Error(),
		}, err
	}

	return &master.NotifyResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}, nil
}

func (g *GrpcService) handleRegisterOperation(ctx context.Context, request *master.NotifyRequest) (*master.NotifyResponse, error) {
	var err error
	var id string
	var resp *master.NotifyResponse
	var respPayload *master.RegisteredPayload
	var any *anypb.Any

	payload := new(master.RegisterPayload)

	if err = request.Payload.UnmarshalTo(payload); err != nil {
		goto RespondFailed
	}

	id = g.m.register(payload)
	resp = &master.NotifyResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}
	respPayload = &master.RegisteredPayload{Identifier: id}
	any, err = anypb.New(respPayload)
	if err != nil {
		goto RespondFailed
	}
	resp.Payload = any

	return resp, nil

RespondFailed:
	return &master.NotifyResponse{
		Succeed:   false,
		Timestamp: timestamppb.Now(),
		Message:   err.Error(),
	}, err
}
