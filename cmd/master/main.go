package main

import (
	"flag"
	"log"

	"github.com/suenchunyu/map-reduce/internal/config"
	"github.com/suenchunyu/map-reduce/internal/master"
)

const (
	defaultConfigFilename = "master.yaml"
)

var (
	configFileName = flag.String("config", defaultConfigFilename, "master config filename")
)

func main() {
	flag.Parse()

	c := new(config.Config)

	if err := config.Load(*configFileName, c); err != nil {
		log.Fatalln(err)
	}

	m, err := master.New(c)
	if err != nil {
		log.Fatalln(err)
	}

	if err := m.Start(); err != nil {
		panic(err)
	}
}
