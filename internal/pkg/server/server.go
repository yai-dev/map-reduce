package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

type Flag uint8

const (
	FlagMaster Flag = iota
	FlagWorker
)

func (f Flag) String() string {
	switch f {
	case FlagMaster:
		return "master"
	case FlagWorker:
		return "worker"
	default:
		return fmt.Sprintf("unknwon (%d)", f)
	}
}

func FlagFromString(str string) Flag {
	switch strings.ToLower(str) {
	case "master":
		return FlagMaster
	case "worker":
		return FlagWorker
	default:
		return FlagMaster
	}
}

type Network uint8

func (n Network) String() string {
	switch n {
	case NetworkTCP:
		return "tcp"
	case NetworkTCP4:
		return "tcp4"
	case NetworkUnix:
		return "unix"
	default:
		return fmt.Sprintf("unknwon (%d)", n)
	}
}

func NetworkFromString(str string) Network {
	switch strings.ToLower(str) {
	case "tcp":
		return NetworkTCP
	case "tcp4":
		return NetworkTCP4
	case "unix":
		return NetworkUnix
	default:
		return NetworkUnix
	}
}

const (
	NetworkTCP Network = iota
	NetworkTCP4
	NetworkUnix
)

const (
	defaultUnixSocketName = "map-reduce-temporary-"
	defaultUnixSocketPath = "/var/tmp"
)

var defaultUnixSocket = defaultUnixSocketPath + "/" + defaultUnixSocketName

type Option func(server *Server) error

func WithAddr(addr string) Option {
	return func(server *Server) error {
		if addr == "" && server.network == NetworkUnix {
			addr = DefaultSocketName()
		}
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

func WithNetwork(net Network) Option {
	return func(server *Server) error {
		server.network = net
		return nil
	}
}

type StopHookFunc func() error

type Server struct {
	flag     Flag
	network  Network
	addr     string
	port     int
	listener net.Listener
	server   *grpc.Server
	hook     StopHookFunc
}

func New(opts ...Option) *Server {
	server := &Server{
		server: grpc.NewServer(),
	}

	for _, opt := range opts {
		if err := opt(server); err != nil {
			panic(err)
		}
	}

	return server
}

func (s *Server) Raw() *grpc.Server {
	return s.server
}

func (s *Server) StopHook(hookFunc StopHookFunc) {
	s.hook = hookFunc
}

func (s *Server) Start() error {

	if s.hook == nil {
		s.hook = func() error {
			return nil
		}
	}

	var listener net.Listener
	var err error

	var address string
	switch s.network {
	case NetworkTCP:
		address = fmt.Sprintf("%s:%d", s.addr, s.port)
	case NetworkTCP4:
		address = fmt.Sprintf("%s:%d", s.addr, s.port)
	case NetworkUnix:
		address = s.addr
	}

	listener, err = net.Listen(s.network.String(), address)
	if err != nil {
		return err
	}
	s.listener = listener

	go func(server *grpc.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil {
			log.Fatalf("shutting down the gRPC server with unexpected error: %s", err)
		}
	}(s.server, s.listener)

	log.Printf("server %s listen and serving at: %s:%d\n", s.flag, s.addr, s.port)

	// graceful stop
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Kill)
	<-quit

	s.server.GracefulStop()
	return s.hook()
}

func DefaultSocketName() string {
	n := defaultUnixSocket
	n += strconv.Itoa(os.Getuid())
	return n
}
