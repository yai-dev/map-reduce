package main

import (
	"strings"
	"unicode"

	"github.com/suenchunyu/map-reduce/pkg/worker/context"
)

const Version = "1.0.0"

type WordCountPlugin struct{}

var Plugin WordCountPlugin

func (w *WordCountPlugin) Version() string {
	return Version
}

func (w *WordCountPlugin) Map(ctx context.Context) error {
	words := strings.FieldsFunc(string(ctx.Content()), func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	for _, word := range words {
		pair := &context.Pair{
			Key:   word,
			Value: 1,
		}
		if err := ctx.Emit(pair); err != nil {
			return err
		}
	}

	return nil
}

func (w *WordCountPlugin) Reduce(ctx context.Context) error {
	keyValues := ctx.KeyValues()
	pair := &context.Pair{
		Key:   keyValues.Key,
		Value: len(keyValues.Values),
	}
	return ctx.Emit(pair)
}
