package master

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/suenchunyu/map-reduce/internal/config"
	"github.com/suenchunyu/map-reduce/internal/model"
	"github.com/suenchunyu/map-reduce/internal/pkg/master"
	"github.com/suenchunyu/map-reduce/internal/pkg/server"
	workerapi "github.com/suenchunyu/map-reduce/internal/pkg/worker"
	"github.com/suenchunyu/map-reduce/pkg/snowflake"
	"google.golang.org/grpc"
)

// worker represents a work process running in the system,
// it is responsible for constantly requesting tasks from
// the Master and executing them, then persisting the result
// after the task is executed and notifying the Master and
// requesting a new task, if there is no task, the worker
// will be in an idle state.
type worker struct {
	id         string                        // worker's unique identifier
	host       string                        // worker's host address
	port       uint32                        // worker's port
	running    uint64                        // running job counts at worker
	finished   uint64                        // finished job counts at worker
	assigned   *model.Task                   // job that currently assigned to worker
	reportedAt int64                         // worker's last report timestamp
	client     workerapi.WorkerServiceClient // worker client
}

type (
	workers    map[string]*worker
	typedTasks map[master.TaskType][]*model.Task
	tasks      map[string]*model.Task
)

// Master represents the coordinator of the Map-Reduce cluster,
// which is responsible for coordinating the scheduling of workers
// and monitoring the health of workers, and maintains a table of
// workers that serves as a registry.
type Master struct {
	mu     *sync.RWMutex  // Mutex for workers table.
	server *server.Server // Server instance for Master.

	expired         time.Duration // Worker expired interval.
	evict           time.Duration // Master eviction routine interval.
	limit           int           // Max evicted workers num for every tick.
	registryEnabled bool          // true if registry has enabled.

	idle workers // idle workers.
	busy workers // busy workers.

	tasks          typedTasks // all tasks.
	assignedMap    tasks      // assigned Map tasks.
	assignedReduce tasks      // assigned Reduce tasks.
	finishedMap    tasks      // finished Map tasks.
	finishedReduce tasks      // finished Reduce tasks.

	minio      *minio.Client // MinIO client used by Master.
	taskBucket string        // Task files bucket name.

	node *snowflake.Node // snowflake identifier node.
}

func New(c *config.Config) (*Master, error) {
	m := &Master{
		mu:             new(sync.RWMutex),
		idle:           make(workers),
		busy:           make(workers),
		tasks:          make(typedTasks),
		assignedMap:    make(tasks),
		assignedReduce: make(tasks),
		finishedMap:    make(tasks),
		finishedReduce: make(tasks),
		taskBucket:     c.TaskBucket,
	}

	s := server.New(
		server.WithNetwork(server.NetworkFromString(c.Network)),
		server.WithFlag(server.FlagFromString(c.Type)),
		server.WithAddr(c.Host),
		server.WithPort(c.Port),
	)
	m.server = s

	node, err := snowflake.NewNode(c.Master.Snowflake.NodeID)
	if err != nil {
		return nil, err
	}
	m.node = node

	m.registryEnabled = c.Master.Registry.Enabled
	if m.registryEnabled {
		evict, err := time.ParseDuration(c.Master.Registry.Expiry.ExpiredEvictInterval)
		if err != nil {
			return nil, err
		}
		m.evict = evict

		expired, err := time.ParseDuration(c.Master.Registry.Expiry.ExpireDuration)
		if err != nil {
			return nil, err
		}
		m.expired = expired

		m.limit = c.Master.Registry.Expiry.ExpiryLimit
	}

	// init the MinIO client
	client, err := minio.New(c.S3Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(c.AccessKey, c.AccessSecret, ""),
	})
	if err != nil {
		return nil, err
	}
	m.minio = client

	return m, nil
}

func (m *Master) Start() error {
	// register gRPC service into gRPC server.
	master.RegisterMasterServiceServer(
		m.server.Raw(),
		&GrpcService{
			m: m,
		},
	)

	if m.registryEnabled {
		go m.evictExpired()
	}

	// init tasks from MinIO
	if err := m.initTasks(); err != nil {
		return err
	}

	return m.server.Start()
}

func (m *Master) initTasks() error {
	c := m.minio

	stdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	objc := c.ListObjects(stdCtx, m.taskBucket, minio.ListObjectsOptions{})

	for obj := range objc {
		task := &model.Task{
			ID:         m.node.Generate().String(),
			Object:     obj.Key,
			Flag:       model.FlagMap,
			Finished:   false,
			FinishedAt: 0,
		}

		if m.tasks[master.TaskType_TASK_TYPE_MAP] == nil {
			m.tasks[master.TaskType_TASK_TYPE_MAP] = make([]*model.Task, 0)
		}

		m.tasks[master.TaskType_TASK_TYPE_MAP] = append(m.tasks[master.TaskType_TASK_TYPE_MAP], task)
		log.Printf("init task from {bucket: %s, object: %s, size: %d}\n", m.taskBucket, obj.Key, obj.Size)
	}

	return nil
}

func (m *Master) ping(payload *master.WorkerReportPayload) error {
	var worker *worker
	var exist bool
	m.mu.RLock()
	worker, exist = m.idle[payload.Identifier]
	if !exist {
		worker, exist = m.busy[payload.Identifier]
		if !exist {
			m.mu.RUnlock()
			return errors.New("could not found the worker")
		}
	}
	m.mu.RUnlock()

	worker.host = payload.Host
	worker.port = payload.Port
	worker.finished = payload.Finished
	worker.running = payload.Running
	worker.reportedAt = time.Now().UnixNano()

	log.Printf("worker {id: %s, host: %s, port: %d, finished: %d, running: %d, reported_at: %d} send heartbeat to master.", worker.id, worker.host, worker.port, worker.finished, worker.running, worker.reportedAt)

	return nil
}

func (m *Master) register(payload *master.RegisterPayload) (string, error) {
	worker := new(worker)
	worker.id = m.node.Generate().String()
	worker.host = payload.Host
	worker.port = payload.Port
	worker.finished = 0
	worker.running = 0

	// FIXME: handling when worker network is unix socket
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", worker.host, worker.port), grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	worker.client = workerapi.NewWorkerServiceClient(conn)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.idle[worker.id] = worker
	log.Printf("new worker has registered: {id: %s, host: %s, port: %d, finished: %d, running: %d}\n", worker.id, worker.host, worker.port, worker.finished, worker.running)
	return worker.id, nil
}

func (m *Master) unregister(id string) error {
	var idle bool
	m.mu.RLock()
	worker, exist := m.idle[id]
	if !exist {
		worker, exist = m.busy[id]
		if !exist {
			m.mu.RUnlock()
			return errors.New("could not found the worker")
		} else {
			idle = false
		}
		idle = true
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if idle {
		delete(m.idle, worker.id)
	} else {
		delete(m.busy, worker.id)
	}

	return nil
}

func (m *Master) evictExpired() {
	ticker := time.NewTicker(m.evict)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("start expired worker checking...")
			m.eviction()
		}
	}
}

func (m *Master) eviction() {
	now := time.Now().UnixNano()

	idle, busy := m.getAllWorkers()

	expired := make([]*worker, 0)

	for _, worker := range idle {
		diff := now - worker.reportedAt
		if diff > m.expired.Nanoseconds() {
			expired = append(expired, worker)
		}
	}

	for _, worker := range busy {
		diff := now - worker.reportedAt
		if diff > m.expired.Nanoseconds() {
			expired = append(expired, worker)
		}
	}

	count := 0
	// Shuffle and eviction
	for i := 0; i < len(expired); i++ {
		ran := i + rand.Intn(len(expired)-1)
		expired[i], expired[ran] = expired[ran], expired[i]
		// FIXME: reassign the evicted busy worker's task
		unregistered := expired[i]
		log.Printf("unregister and eviction worker: %s\n", unregistered.id)
		_ = m.unregister(unregistered.id)
		count++
	}

	log.Printf("end of expired worker checking, %d workers has evicted.", count)
}

func (m *Master) acquire(workerID string, taskType master.TaskType) (*model.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, exist := m.idle[workerID]
	if !exist {
		w, exist = m.busy[workerID]
		if !exist {
			return nil, errors.New("worker not found")
		} else {
			return nil, errors.New("worker is busy")
		}
	}

	deepcopy := func(src *model.Task) *model.Task {
		dst := new(model.Task)
		*dst = *src
		return dst
	}

	var target *model.Task
	for idx, task := range m.tasks[taskType] {
		var exist bool

		switch taskType {
		case master.TaskType_TASK_TYPE_MAP:
			_, exist = m.assignedMap[task.ID]
			if !exist {
				_, exist = m.finishedMap[task.ID]
			}
		case master.TaskType_TASK_TYPE_REDUCE:
			_, exist = m.assignedReduce[task.ID]
			if !exist {
				_, exist = m.finishedReduce[task.ID]
			}
		}

		if exist {
			continue
		}

		target = deepcopy(task)
		m.tasks[taskType] = append(m.tasks[taskType][:idx], m.tasks[taskType][idx+1:]...)

		break
	}

	if target == nil {
		return nil, errors.New("no available task")
	}

	w.assigned = target

	m.busy[w.id] = w
	delete(m.idle, w.id)

	switch taskType {
	case master.TaskType_TASK_TYPE_MAP:
		m.assignedMap[target.ID] = target
	case master.TaskType_TASK_TYPE_REDUCE:
		m.assignedReduce[target.ID] = target
	}

	log.Printf("assigned task {id: %s, object: %s, flag: %s} to worker {id: %s}", target.ID, target.Object, target.Flag, w.id)
	return target, nil
}

func (m *Master) completion(workerID string, finished bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, exist := m.busy[workerID]
	if !exist {
		return errors.New("worker not found")
	}

	assigned := w.assigned
	w.assigned = nil

	if finished {
		assigned.FinishedAt = time.Now().UnixNano()

		switch assigned.Flag {
		case model.FlagMap:
			m.finishedMap[assigned.ID] = assigned
			delete(m.assignedMap, assigned.ID)
		case model.FlagReduce:
			m.finishedReduce[assigned.ID] = assigned
			delete(m.assignedReduce, assigned.ID)
		}

		if len(m.tasks) == 0 {
			log.Printf("ready to switch the worker {id: %s} 's mode to Reduce...\n", w.id)
			// notification workers switch to Reduce mode
			stdCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			request := &workerapi.ChangeModeRequest{
				TargetMode: workerapi.WorkerMode_MODE_REDUCE,
			}

			_, err := w.client.ChangeWorkerMode(stdCtx, request)
			if err != nil {
				// rollback op
				switch assigned.Flag {
				case model.FlagMap:
					delete(m.finishedMap, assigned.ID)
					delete(m.assignedMap, assigned.ID)
				case model.FlagReduce:
					delete(m.finishedReduce, assigned.ID)
					delete(m.assignedReduce, assigned.ID)
				}

				return err
			}
		}

		m.idle[w.id] = w
		delete(m.busy, w.id)

		log.Printf("worker {id: %s} completed the task {id: %s, object: %s, flag: %s}", w.id, assigned.ID, assigned.Object, assigned.Flag)
		return nil
	} else {
		switch assigned.Flag {
		case model.FlagMap:
			delete(m.assignedMap, assigned.ID)
		case model.FlagReduce:
			delete(m.assignedReduce, assigned.ID)
		}

		m.idle[w.id] = w
		delete(m.busy, w.id)

		log.Printf("worker {id: %s} uncompleted the task {id: %s, object: %s, flag: %s}", w.id, assigned.ID, assigned.Object, assigned.Flag)
		return nil
	}
}

func (m *Master) reverseAssigned(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w := m.busy[workerID]
	target := w.assigned
	w.assigned = nil

	m.idle[workerID] = w
	delete(m.busy, workerID)

	switch target.Flag {
	case model.FlagMap:
		delete(m.assignedMap, target.ID)
	case model.FlagReduce:
		delete(m.assignedReduce, target.ID)
	}

	m.tasks[taskTypeFromFlag(target.Flag)] = append(m.tasks[taskTypeFromFlag(target.Flag)], target)
}

func (m *Master) getAllWorkers() (workers, workers) {
	m.mu.Lock()
	defer m.mu.Unlock()

	deepcopy := func(src *worker) *worker {
		dst := new(worker)
		*dst = *src
		return dst
	}

	busy := make(workers)
	for key, worker := range m.busy {
		busy[key] = deepcopy(worker)
	}

	idle := make(workers)
	for key, worker := range m.idle {
		idle[key] = deepcopy(worker)
	}

	return idle, busy
}
