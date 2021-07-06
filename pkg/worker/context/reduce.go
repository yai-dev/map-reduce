package context

import (
	stdCtx "context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
)

type ReduceContext struct {
	id           string
	obj          string
	content      []byte
	fn           string
	fp           *os.File
	dec          *gob.Decoder
	enc          *gob.Encoder
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

	ctx.dec = gob.NewDecoder(object)
	pairs := make([]*Pair, 0)

	if err = ctx.dec.Decode(&pairs); err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(object)
	if err != nil {
		return nil, err
	}
	ctx.content = bytes

	ctx.shuffle(pairs)

	ctx.fn = fmt.Sprintf("%s%s-%s", ctx.reducePrefix, ctx.obj, id)
	fp, err := ioutil.TempFile("/var/tmp", ctx.fn)
	if err != nil {
		return nil, err
	}

	ctx.fp = fp
	ctx.enc = gob.NewEncoder(fp)

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

func (r ReduceContext) Emit(pair *Pair) error {
	return r.enc.Encode(pair)
}

func (r ReduceContext) KeyValues() *KeyValues {
	return r.values
}

func (r ReduceContext) Release() error {
	defer func() {
		_ = r.fp.Close()
	}()

	stat, err := r.fp.Stat()
	if err != nil {
		return err
	}

	if stat.Size() != 0 {
		context, cancel := stdCtx.WithTimeout(stdCtx.Background(), 5*time.Second)
		defer cancel()

		_, err = r.minio.PutObject(context, r.reduceBucket, r.fn, r.fp, stat.Size(), minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}
