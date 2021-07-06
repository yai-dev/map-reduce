package context

import (
	"bytes"
	stdCtx "context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/minio/minio-go/v7"
)

type MapContext struct {
	id          string
	obj         string
	content     []byte
	pairs       []*Pair
	pairsMap    map[string][]*Pair
	minio       *minio.Client
	interBucket string
	interPrefix string
}

var _ Context = new(MapContext)

func NewMapContext(c *minio.Client, obj, bucket, id, interBucket, interPrefix string) (Context, error) {
	ctx := &MapContext{
		id:          id,
		obj:         obj,
		minio:       c,
		pairs:       make([]*Pair, 0),
		pairsMap:    make(map[string][]*Pair),
		interBucket: interBucket,
		interPrefix: interPrefix,
	}

	context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)
	defer cancel()

	object, err := c.GetObject(context, bucket, ctx.obj, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(object)
	if err != nil {
		return nil, err
	}

	ctx.content = bytes

	return ctx, nil
}

func (m MapContext) ObjectName() string {
	return m.obj
}

func (m MapContext) Content() []byte {
	return m.content
}

func (m MapContext) Pairs() []*Pair {
	return m.pairs
}

func (m *MapContext) Emit(pair *Pair) error {
	m.pairs = append(m.pairs, pair)

	if pairs, ok := m.pairsMap[pair.Key]; !ok {
		p := make([]*Pair, 0)
		p = append(p, pair)
		m.pairsMap[pair.Key] = p
	} else {
		m.pairsMap[pair.Key] = append(pairs, pair)
	}

	return nil
}

func (m MapContext) KeyValues() *KeyValues {
	panic("unsupported operation in Map Context")
}

func (m MapContext) Release() error {
	for key, pairs := range m.pairsMap {
		fn := fmt.Sprintf("%s%s-%s", m.interPrefix, key, m.id)

		jsonBytes, err := json.Marshal(pairs)
		if err != nil {
			return err
		}

		buffer := bytes.NewBuffer(jsonBytes)

		context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)

		_, err = m.minio.PutObject(context, m.interBucket, fn, buffer, int64(buffer.Len()), minio.PutObjectOptions{})
		if err != nil {
			cancel()
			return err
		}

		cancel()
	}

	return nil
}
