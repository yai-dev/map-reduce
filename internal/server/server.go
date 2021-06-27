package server

import (
    "net/http"
    "net/rpc"
    "os"
    "strconv"
    "sync"
)

type Flag uint8

const (
    Master Flag = iota
    Worker
)

type Option func(server *Server) error

func WithAddr(addr string) Option {
    return func(server *Server) error {
        server.addr = addr
        return nil
    }
}

func WithPort(port int) Option {
    return func(server *Server) error {
        server.port = port
        return nil
    }
}

func WithFlag(flag Flag) Option {
    return func(server *Server) error {
        server.flag = flag
        return nil
    }
}

func WithNetwork(net string) Option {
    return func(server *Server) error {
        server.network = net
        return nil
    }
}

type Server struct {
    mutex *sync.Mutex

    network string
    flag    Flag
    addr    string
    port    int
    serv    *http.Server
    svc     *rpc.Server
}

func New(opts ...Option) *Server {
    if len(opts) == 0 {
        opts = []Option{
            WithFlag(Master),
            WithNetwork("unix"),
            WithAddr(defaultSocketName()),
        }
    }

    server := &Server{
        mutex: new(sync.Mutex),
    }

    for _, opt := range opts {
        if err := opt(server); err != nil {
            panic(err)
        }
    }

    return server
}

func (s *Server) Start() error {
    return nil
}

func defaultSocketName() string {
    n := "/var/tmp/mr-824-"
    n += strconv.Itoa(os.Getuid())
    return n
}
