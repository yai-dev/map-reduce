package plugin

import (
	"plugin"

	"github.com/suenchunyu/map-reduce/pkg/worker/context"
)

type Plugin interface {
	Version() string
	Map(ctx context.Context) error
	Reduce(ctx context.Context) error
}

func Load(path string) (Plugin, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	symbol, err := p.Lookup("Plugin")
	if err != nil {
		return nil, err
	}

	return symbol.(Plugin), err
}
