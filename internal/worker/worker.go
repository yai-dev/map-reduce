package worker

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/suenchunyu/map-reduce/internal/config"
	"github.com/suenchunyu/map-reduce/internal/master"
	"github.com/suenchunyu/map-reduce/internal/model"
	"github.com/suenchunyu/map-reduce/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrAlreadyRegistered = errors.New("already registered")
)

type Worker struct {
	mu *sync.Mutex

	id   string
	task *model.Task

	running  uint64
	finished uint64

	master     string
	registered bool

	client    master.MasterServiceClient
	heartbeat time.Duration
	acquire   time.Duration

	stop   context.CancelFunc
	notify chan struct{}
	plugin worker.Plugin
}

func New(c *config.Config) (*Worker, error) {
	heartbeat, err := time.ParseDuration(c.Worker.HeartbeatDuration)
	if err != nil {
		return nil, err
	}

	acquire, err := time.ParseDuration(c.Worker.AcquireTaskDuration)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		mu:        new(sync.Mutex),
		heartbeat: heartbeat,
		acquire:   acquire,
		notify:    make(chan struct{}, 1),
	}

	// load Map-Reduce plugin
	if c.Worker.Plugin.Enabled {
		p, err := worker.Load(c.Worker.Plugin.Path)
		if err != nil {
			return nil, err
		}
		w.plugin = p
	}

	return w, nil
}

func (w *Worker) Start() error {
	// init master gRPC service client.
	if err := w.initMasterClient(); err != nil {
		return err
	}

	// register self to master.
	if err := w.register(); err != nil {
		return err
	}

	// init the context
	ctx, cancel := context.WithCancel(context.Background())
	w.stop = cancel

	// running the goroutine for heart beating.
	go w.ping(ctx)

	// running the goroutine for acquire the task from master.
	go w.acquiring(ctx)

	// running the goroutine for processing task with loaded plugin.
	go w.run(ctx)

	// graceful stop
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, os.Kill, os.Interrupt)
	// wait the signal from os
	<-signalC

	// release the worker
	if err := w.release(); err != nil {
		return err
	}

	return nil
}

func (w *Worker) initMasterClient() error {
	conn, err := grpc.Dial(w.master, grpc.WithInsecure())
	if err != nil {
		return err
	}

	w.client = master.NewMasterServiceClient(conn)
	return nil
}

func (w *Worker) register() error {
	if w.registered {
		return ErrAlreadyRegistered
	}

	c := w.client

	// FIXME: currently, the worker doesn't need the gRPC services, so the host and port is hardcoded.
	payload := &master.RegisterPayload{
		Host: "mocked-host",
		Port: 8888,
	}

	any, err := anypb.New(payload)
	if err != nil {
		return err
	}

	request := &master.NotifyRequest{
		Op:      master.NotifyOp_OP_REGISTER,
		Payload: any,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.Notify(ctx, request)
	if err != nil {
		return err
	}

	if !resp.Succeed {
		return errors.New(resp.Message)
	}

	respPayload := new(master.RegisteredPayload)
	if err := resp.Payload.UnmarshalTo(respPayload); err != nil {
		return err
	}

	w.id = respPayload.Identifier
	w.registered = true

	return nil
}

func (w *Worker) unregister() error {
	panic("implemented me")
}

func (w *Worker) ping(ctx context.Context) {
	c := w.client
	ticker := time.NewTicker(w.heartbeat)
	defer ticker.Stop()
PingLoop:
	for {
		select {
		case <-ctx.Done():
			break PingLoop
		case <-ticker.C:
			running := atomic.LoadUint64(&w.running)
			finished := atomic.LoadUint64(&w.finished)

			payload := &master.WorkerReportPayload{
				Identifier: w.id,
				Host:       "mocked-host",
				Port:       8888,
				Running:    running,
				Finished:   finished,
				Timestamp:  timestamppb.Now(),
			}

			any, _ := anypb.New(payload)

			request := &master.NotifyRequest{
				Op:      master.NotifyOp_OP_HEARTBEAT,
				Payload: any,
			}

			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)

			// avoid the useless response.
			_, err := c.Notify(ctx, request)
			if err != nil {
				cancel()
				continue
			}

			cancel()
		default:
			// do nothing, continue the next ping loop.
		}
	}
}

func (w *Worker) acquiring(ctx context.Context) {
	c := w.client
	ticker := time.NewTicker(w.acquire)
	defer ticker.Stop()

AcquiringLoop:
	for {
		select {
		case <-ctx.Done():
			break AcquiringLoop
		case <-ticker.C:
			payload := &master.TaskAcquirePayload{
				// FIXME: master need to notify the worker change the acquire task type.
				TaskType:         master.TaskType_TASK_TYPE_MAP,
				WorkerIdentifier: w.id,
			}

			any, _ := anypb.New(payload)

			request := &master.NotifyRequest{
				Op:      master.NotifyOp_OP_ACQUIRE_TASK,
				Payload: any,
			}

			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)

			resp, err := c.Notify(ctx, request)
			if err != nil {
				cancel()
				continue
			}

			respPayload := new(master.TaskPayload)
			if err := resp.Payload.UnmarshalTo(respPayload); err != nil {
				cancel()
				continue
			}

			task := &model.Task{
				Object:     respPayload.FileSource,
				Flag:       model.FlagMapFromProtoBufferType(respPayload.Type),
				Finished:   false,
				FinishedAt: 0,
			}

			w.mu.Lock()
			// update the task.
			w.task = task
			// notify the background run routine
			w.notify <- struct{}{}
			w.mu.Unlock()
			// cancel the context avoid the context leak.
			cancel()

		default:
			// do nothing, continue the next acquiring loop.
		}
	}
}

func (w *Worker) run(ctx context.Context) {
ProcessingLoop:
	for {
		select {
		case <-ctx.Done():
			break ProcessingLoop
		case <-w.notify:
		default:
			continue
		}
	}
}

func (w *Worker) release() error {
	// stop the all goroutine running at background
	w.stop()
	// TODO: wait the task completed

	// TODO: unregister from master.

	return nil
}
