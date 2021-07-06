package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/suenchunyu/map-reduce/internal/config"
	"github.com/suenchunyu/map-reduce/internal/model"
	"github.com/suenchunyu/map-reduce/internal/pkg/master"
	"github.com/suenchunyu/map-reduce/internal/pkg/server"
	"github.com/suenchunyu/map-reduce/internal/pkg/worker"
	workerCtx "github.com/suenchunyu/map-reduce/pkg/worker/context"
	"github.com/suenchunyu/map-reduce/pkg/worker/plugin"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrAlreadyRegistered = errors.New("already registered")
	ErrUnregistered      = errors.New("unregistered")
)

type Mode uint8

const (
	ModeUnknown Mode = iota
	ModeMap
	ModeReduce
)

func modeFromWorkerProtoBuffer(mode worker.WorkerMode) Mode {
	switch mode {
	case worker.WorkerMode_MODE_UNKNOWN:
		return ModeUnknown
	case worker.WorkerMode_MODE_MAP:
		return ModeMap
	case worker.WorkerMode_MODE_REDUCE:
		return ModeReduce
	default:
		return ModeUnknown
	}
}

func (m Mode) getTaskType() master.TaskType {
	switch m {
	case ModeMap:
		return master.TaskType_TASK_TYPE_MAP
	case ModeReduce:
		return master.TaskType_TASK_TYPE_REDUCE
	default:
		return master.TaskType_TASK_TYPE_MAP
	}
}

// Worker represents a work process running in the system,
// it is responsible for constantly requesting tasks from
// the Master and executing them, then persisting the result
// after the task is executed and notifying the Master and
// requesting a new task, if there is no task, the worker will
// be in an idle state.
type Worker struct {
	mu *sync.Mutex // Mutex for protect the task field.

	id     string         // Worker unique identifier assigned by Master.
	addr   string         // Address for Worker.
	port   int            // Port listening on for Worker.
	mode   Mode           // Worker processing mode.
	server *server.Server // gRPC server instance for current Worker.

	task *model.Task // Currently task assigned to current Worker.

	running  uint64 // Running task counts at current Worker.
	finished uint64 // Finished task counts at current Worker.

	master     string // Master address.
	registered bool   // True if current Worker has been registered to Master.

	client master.MasterServiceClient // Master service client using by current Worker.

	heartbeat time.Duration      // Heartbeat request interval.
	acquire   time.Duration      // Acquire task request interval.
	stop      context.CancelFunc // Context cancel function.
	notify    chan struct{}      // notify used for notification the run goroutine when task has been updated.

	plugin plugin.Plugin // Loaded plugin handle for current Worker.

	minio              *minio.Client // Minio client using by current Worker.
	taskBucket         string        // Task file bucket.
	intermediateBucket string        // Intermediate file bucket.
	intermediatePrefix string        // Intermediate file prefix.
	resultBucket       string        // Result file bucket.
	resultPrefix       string        // Result file prefix.
}

func New(c *config.Config) (*Worker, error) {
	// transformation the durations.
	heartbeat, err := time.ParseDuration(c.Worker.HeartbeatDuration)
	if err != nil {
		return nil, err
	}

	acquire, err := time.ParseDuration(c.Worker.AcquireTaskDuration)
	if err != nil {
		return nil, err
	}

	// create the Worker instance.
	w := &Worker{
		mu:                 new(sync.Mutex),
		mode:               ModeMap,
		heartbeat:          heartbeat,
		acquire:            acquire,
		notify:             make(chan struct{}, 1),
		addr:               c.Host,
		port:               c.Port,
		taskBucket:         c.TaskBucket,
		intermediateBucket: c.IntermediateBucket,
		intermediatePrefix: c.IntermediatePrefix,
		resultBucket:       c.ResultBucket,
		resultPrefix:       c.ResultPrefix,
	}

	// load Map-Reduce plugin.
	if c.Worker.Plugin.Enabled {
		p, err := plugin.Load(c.Worker.Plugin.Path)
		if err != nil {
			return nil, err
		}
		w.plugin = p
	}

	// create the gRPC server.
	s := server.New(
		server.WithNetwork(server.NetworkFromString(c.Network)),
		server.WithAddr(c.Host),
		server.WithPort(c.Port),
		server.WithFlag(server.FlagFromString(c.Type)),
	)
	s.StopHook(w.release)
	w.server = s

	// init the MinIO client
	client, err := minio.New(c.S3Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(c.AccessKey, c.AccessSecret, ""),
	})
	if err != nil {
		return nil, err
	}
	w.minio = client

	return w, nil
}

func (w *Worker) Start() error {
	// register the gRPC service
	worker.RegisterWorkerServiceServer(
		w.server.Raw(),
		&GrpcService{
			w: w,
		},
	)

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

	return w.server.Start()
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

	payload := &master.RegisterPayload{
		Host: w.addr,
		Port: uint32(w.port),
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
	if !w.registered {
		return ErrUnregistered
	}

	c := w.client

	payload := &master.UnregisterPayload{
		Identifier: w.id,
	}

	any, err := anypb.New(payload)
	if err != nil {
		return err
	}

	request := &master.NotifyRequest{
		Op:      master.NotifyOp_OP_UNREGISTER,
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

	return nil
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
				Host:       w.addr,
				Port:       uint32(w.port),
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
				TaskType:         w.mode.getTaskType(),
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
				ID:         respPayload.TaskIdentifier,
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
			task := w.task
			atomic.StoreUint64(&w.running, 1)

			switch w.mode {
			case ModeMap:
				// create worker context
				mapContext, err := workerCtx.NewMapContext(w.minio, task.Object, task.ID, w.taskBucket, w.intermediateBucket, w.intermediatePrefix)
				if err != nil {
					_ = w.notifyMaster(false)
					continue
				}

				// call the plugin's Map function
				if err = w.plugin.Map(mapContext); err != nil {
					_ = mapContext.Release()
					_ = w.notifyMaster(false)
					continue
				}

				// release the context and persistence the result
				if err = mapContext.Release(); err != nil {
					_ = w.notifyMaster(false)
					continue
				}

				// notification the master task has been done.
				if err = w.notifyMaster(true); err != nil {
					continue
				}
			case ModeReduce:
				// create reduce context
				reduceContext, err := workerCtx.NewReduceContext(w.minio, task.Object, task.ID, w.taskBucket, w.resultBucket, w.resultPrefix)
				if err != nil {
					_ = w.notifyMaster(false)
					continue
				}

				// call the plugin's Reduce function
				if err = w.plugin.Reduce(reduceContext); err != nil {
					_ = reduceContext.Release()
					_ = w.notifyMaster(false)
					continue
				}

				// release the context and persistence the result
				if err = reduceContext.Release(); err != nil {
					_ = w.notifyMaster(false)
					continue
				}

				// notification the master task has been done
				if err = w.notifyMaster(true); err != nil {
					continue
				}
			}
			atomic.StoreUint64(&w.running, 0)
		default:
			continue
		}
	}
}

func (w *Worker) notifyMaster(finished bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.client
	payload := &master.TaskCompletionPayload{
		WorkerIdentifier: w.id,
		TaskIdentifier:   w.task.ID,
		Finished:         finished,
		// FIXME: return errors to master.
	}

	any, err := anypb.New(payload)
	if err != nil {
		return err
	}

	request := &master.NotifyRequest{
		Op:      master.NotifyOp_OP_TASK_COMPLETION,
		Payload: any,
	}

	stdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = c.Notify(stdCtx, request)
	if err != nil {
		return err
	}

	if finished {
		atomic.StoreUint64(&w.finished, w.finished+1)
	}

	return nil
}

func (w *Worker) changeMode(m Mode) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mode == m {
		return
	}

	// FIXME: Has some better way to handling this
	go func() {
		for atomic.LoadUint64(&w.running) != 0 {
			// wait until the running task is empty
		}
		w.mu.Lock()
		defer w.mu.Unlock()

		// update the mode
		w.mode = m
	}()
}

func (w *Worker) release() error {
	//  wait the task completed
	for atomic.LoadUint64(&w.running) != 0 {
		// wait until the running task is empty
	}

	// stop the all goroutine running at background
	w.stop()

	// unregister from master.
	return w.unregister()
}
