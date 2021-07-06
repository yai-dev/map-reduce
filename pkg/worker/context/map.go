package context

import (
	stdCtx "context"
	"encoding/gob"
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
		interBucket: interBucket,
		interPrefix: interPrefix,
	}

	context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)
	defer cancel()

	object, err := c.GetObject(context, ctx.obj, bucket, minio.GetObjectOptions{})
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

func (m MapContext) Emit(pair *Pair) error {
	m.pairs = append(m.pairs, pair)

	if pairs, ok := m.pairsMap[pair.Key]; !ok {
		if pairs == nil {
			pairs = make([]*Pair, 0)
		}

		pairs = append(pairs, pair)
	}

	return nil
}

func (m MapContext) KeyValues() *KeyValues {
	panic("unsupported operation in Map Context")
}

func (m MapContext) Release() error {
	for key, pairs := range m.pairsMap {
		fn := fmt.Sprintf("%s%s-%s", m.interBucket, key, m.id)
		fp, err := ioutil.TempFile("/var/tmp", fn)
		if err != nil {
			return err
		}

		stat, err := fp.Stat()
		if err != nil {
			_ = fp.Close()
			return err
		}

		enc := gob.NewEncoder(fp)
		if err = enc.Encode(pairs); err != nil {
			_ = fp.Close()
			return err
		}

		context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)

		_, err = m.minio.PutObject(context, m.interBucket, fn, fp, stat.Size(), minio.PutObjectOptions{})
		if err != nil {
			cancel()
			_ = fp.Close()
			return err
		}

		cancel()
		_ = fp.Close()
	}

	return nil
}
