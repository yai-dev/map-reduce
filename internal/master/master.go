package master

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/suenchunyu/map-reduce/internal/config"
	"github.com/suenchunyu/map-reduce/internal/model"
	"github.com/suenchunyu/map-reduce/internal/server"
	"github.com/suenchunyu/map-reduce/pkg/snowflake"
)

// worker represents a work process running in the system,
// it is responsible for constantly requesting tasks from
// the Master and executing them, then persisting the result
// after the task is executed and notifying the Master and
// requesting a new task, if there is no task, the worker
// will be in an idle state.
type worker struct {
	id         string      // worker's unique identifier
	host       string      // worker's host address
	port       uint32      // worker's port
	running    uint64      // running job counts at worker
	finished   uint64      // finished job counts at worker
	assigned   *model.Task // job that currently assigned to worker
	reportedAt int64       // worker's last report timestamp
}

type (
	workers map[string]*worker
	tasks   map[TaskType]*model.Task
)

// Master represents the coordinator of the Map-Reduce cluster,
// which is responsible for coordinating the scheduling of workers
// and monitoring the health of workers, and maintains a table of
// workers that serves as a registry.
type Master struct {
	mu     *sync.RWMutex  // Mutex for workers table
	server *server.Server // Server instance for Master.

	expired         time.Duration // Worker expired interval
	evict           time.Duration // Master eviction routine interval
	limit           int           // Max evicted workers num for every tick
	registryEnabled bool          // true if registry has enabled

	idle  workers // idle workers
	busy  workers // busy workers
	tasks tasks   // all tasks

	node *snowflake.Node // snowflake identifier node
}

func New(c *config.Config) (*Master, error) {
	m := &Master{
		mu:    new(sync.RWMutex),
		idle:  make(workers),
		busy:  make(workers),
		tasks: make(tasks),
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

	return m, nil
}

func (m *Master) Start() error {
	// register gRPC service into gRPC server.
	RegisterMasterServiceServer(
		m.server.Raw(),
		&GrpcService{
			m: m,
		},
	)

	if m.registryEnabled {
		go m.evictExpired()
	}

	return m.server.Start()
}

func (m *Master) ping(payload *WorkerReportPayload) error {
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

	return nil
}

func (m *Master) register(payload *RegisterPayload) string {
	worker := new(worker)
	worker.id = m.node.Generate().String()
	worker.host = payload.Host
	worker.port = payload.Port
	worker.finished = 0
	worker.running = 0

	m.mu.Lock()
	defer m.mu.Unlock()
	m.idle[worker.id] = worker
	return worker.id
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

	// Shuffle and eviction
	for i := 0; i < len(expired); i++ {
		ran := i + rand.Intn(len(expired)-1)
		expired[i], expired[ran] = expired[ran], expired[i]
		// FIXME: reassign the evicted busy worker's task
		unregistered := expired[i]
		log.Printf("unregister and eviction worker: %s\n", unregistered.id)
		_ = m.unregister(unregistered.id)
	}

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
