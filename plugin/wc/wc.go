package wc

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/suenchunyu/map-reduce/pkg/worker"
)

const Version = "1.0.0"

type WordCountPlugin struct{}

func (w *WordCountPlugin) Version() string {
	return Version
}

func (w *WordCountPlugin) Map(ctx *worker.Context) error {
	words := strings.FieldsFunc(string(ctx.Content), func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	pairs := make([]*worker.Pair, 0)

	for _, word := range words {
		pair := &worker.Pair{
			Key:   word,
			Value: 1,
		}
		pairs = append(pairs, pair)
	}

	ctx.Pairs = pairs
	return nil
}

func (w *WordCountPlugin) Reduce(ctx *worker.Context) error {
	ctx.Reduced = strconv.Itoa(len(ctx.Values))
	return nil
}
