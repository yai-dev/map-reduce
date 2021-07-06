package worker

import (
	"flag"
	"log"

	"github.com/suenchunyu/map-reduce/internal/config"
	"github.com/suenchunyu/map-reduce/internal/worker"
)

const (
	defaultConfigFilename = "worker.yaml"
)

var (
	configFileName = flag.String("config", defaultConfigFilename, "worker config filename")
)

func main() {
	flag.Parse()

	c := new(config.Config)

	if err := config.Load(*configFileName, c); err != nil {
		log.Fatalln(err)
	}

	m, err := worker.New(c)
	if err != nil {
		log.Fatalln(err)
	}

	if err := m.Start(); err != nil {
		panic(err)
	}
}
