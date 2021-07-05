package worker

import "plugin"

type Pair struct {
	Key   string
	Value interface{}
}

type Context struct {
	Object  string
	Content []byte
	Pairs   []*Pair
	Reduced interface{}
	Values  []interface{}
}

type Plugin interface {
	Version() string
	Map(ctx *Context) error
	Reduce(ctx *Context) error
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
