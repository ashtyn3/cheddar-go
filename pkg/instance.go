package cheddar

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lotusdblabs/lotusdb/v2"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

type Instance struct {
	path string
	Db   *lotusdb.DB
}

func (i *Instance) New(p string) {
	_, err := os.Stat(p)
	i.path = p
	ops := lotusdb.DefaultOptions
	ops.DirPath = i.path
	d, err := lotusdb.Open(ops)
	if err != nil {
		zap.L().Error("Bad open", zap.Error(err))
	}
	if os.IsNotExist(err) {
		p, _ := filepath.Abs(p)
		zap.L().Info("Created db", zap.String("path", p))
	}
	i.Db = d
}
func (i *Instance) Trace() {
	iter, err := i.Db.NewIterator(lotusdb.IteratorOptions{})
	if err != nil {
		zap.L().Error("Bad iter", zap.Error(err))
	}
	zap.L().Info("TRACE:")
	for iter.Valid() {
		bBuffer := bytes.NewBuffer(iter.Value())
		head := zap.Any("head", ParseHeadBytes(bBuffer))
		zap.L().Info("found:", zap.ByteString("key", iter.Key()), zap.String("value", fmt.Sprint(iter.Value())), head)
		iter.Next()
	}
	iter.Close()
}

func (i *Instance) generateID() string {
	return xid.New().String()
}

type RowSegOptions struct {
	passedId string
}

func (i *Instance) InsertRowSegment(table string, col uint64, val Serial, ops *RowSegOptions) []byte {
	id := i.generateID()
	if ops != nil {
		if len(ops.passedId) != 0 {
			id = ops.passedId
		}
	}
	key := fmt.Sprintf("%s.%d.%s", table, col, id)
	err := i.Db.Put([]byte(key), val.Serialize())
	if err != nil {
		zap.L().Error("Bad put", zap.Error(err))
	}
	return []byte(id)
}

func (i *Instance) GetRowSegment(key []byte) (*RowSegment, error) {
	data, err := i.Db.Get(key)
	if err != nil {
		zap.L().Error("Bad get", zap.Error(err))
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	h := ParseHeadBytes(buf)

	return &RowSegment{
		Key:   ParseKeyBytes(key),
		Head:  h,
		Value: data,
	}, err
}

func (i *Instance) InsertTable(t *Table) error {
	err := i.Db.Put([]byte(string(t.Name)), t.Serialize())
	if err != nil {
		return err
	}

	return nil
}
