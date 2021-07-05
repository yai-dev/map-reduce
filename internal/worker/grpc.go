package worker

import (
	"context"

	"github.com/suenchunyu/map-reduce/internal/pkg/worker"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GrpcService struct {
	w *Worker
}

var _ worker.WorkerServiceServer = new(GrpcService)

func (g GrpcService) ChangeWorkerMode(ctx context.Context, request *worker.ChangeModeRequest) (*worker.ChangeModeResponse, error) {
	g.w.changeMode(modeFromWorkerProtoBuffer(request.TargetMode))

	return &worker.ChangeModeResponse{
		Succeed:   true,
		Timestamp: timestamppb.Now(),
		Message:   "succeed",
	}, nil
}
