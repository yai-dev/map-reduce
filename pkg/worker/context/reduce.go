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

type ReduceContext struct {
	id           string
	obj          string
	content      []byte
	fn           string
	pair         *Pair
	minio        *minio.Client
	reduceBucket string
	reducePrefix string
	values       *KeyValues
}

var _ Context = new(ReduceContext)

func NewReduceContext(c *minio.Client, obj, bucket, id, reduceBucket, reducePrefix string) (Context, error) {
	ctx := &ReduceContext{
		id:           id,
		obj:          obj,
		minio:        c,
		reduceBucket: reduceBucket,
		reducePrefix: reducePrefix,
	}

	context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)
	defer cancel()

	object, err := c.GetObject(context, ctx.obj, bucket, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	pairs := make([]*Pair, 0)
	objBytes, err := ioutil.ReadAll(object)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(objBytes, &pairs); err != nil {
		return nil, err
	}

	ctx.content = objBytes

	ctx.shuffle(pairs)

	ctx.fn = fmt.Sprintf("%s%s-%s", ctx.reducePrefix, ctx.obj, id)

	return ctx, nil
}

func (r *ReduceContext) shuffle(pairs []*Pair) {
	r.values = new(KeyValues)
	for _, pair := range pairs {
		if r.values.Key == "" {
			r.values.Key = pair.Key
		}

		if r.values.Values == nil {
			r.values.Values = make([]interface{}, 0)
		}

		r.values.Values = append(r.values.Values, pair.Value)
	}
}

func (r ReduceContext) ObjectName() string {
	return r.obj
}

func (r ReduceContext) Content() []byte {
	return r.content
}

func (r ReduceContext) Pairs() []*Pair {
	panic("unsupported operation in Reduce Context")
}

func (r *ReduceContext) Emit(pair *Pair) error {
	r.pair = pair
	return nil
}

func (r ReduceContext) KeyValues() *KeyValues {
	return r.values
}

func (r ReduceContext) Release() error {
	if r.pair == nil {
		context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)
		defer cancel()

		jsonBytes, err := json.Marshal(r.pair)
		if err != nil {
			return err
		}

		buff := bytes.NewBuffer(jsonBytes)

		_, err = r.minio.PutObject(context, r.reduceBucket, r.fn, buff, int64(buff.Len()), minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}
