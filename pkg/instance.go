package cheddar

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cespare/xxhash/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/lotusdblabs/lotusdb/v2"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

type Cacheable struct {
	Id string
}

type Instance struct {
	path  string
	Db    *lotusdb.DB
	cache *lru.TwoQueueCache[uint64, Cacheable]
	Pool  *pool
}

var (
	NoColFound = errors.New("Could not find column by name")
)

func (i *Instance) New(p string) *Instance {
	_, err := os.Stat(p)
	i.path = p
	ops := lotusdb.DefaultOptions
	ops.DirPath = i.path
	d, err := lotusdb.Open(ops)
	if err != nil {
		log.Fatal().Err(err).Msg("Bad open")
	}
	if os.IsNotExist(err) {
		p, _ := filepath.Abs(p)
		log.Info().Str("path", p).Msg("Created db")
	}
	c, err := lru.New2Q[uint64, Cacheable](500)
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot allocate cache")
	}
	i.cache = c
	i.Db = d
	i.Pool = new(pool).New()
	return i
}

func (i *Instance) Trace() {
	iter, err := i.Db.NewIterator(lotusdb.IteratorOptions{})
	if err != nil {
		log.Fatal().Err(err).Msg("Bad iter")
	}
	log.Info().Msg("TRACE:")
	for iter.Valid() {
		log.Info().Bytes("key", iter.Key()).Str("value", fmt.Sprint(iter.Value())).Msg("found")
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
		log.Error().Err(err).Msg("Bad put")
	}
	return []byte(id)
}

func (i *Instance) GetRowSegment(key []byte) (*RowSegment, error) {
	data, err := i.Db.Get(key)
	if err != nil {
		log.Error().Err(err).Msg("Bad get")
		return nil, err
	}
	buf := i.Pool.newBuffer(data)
	h := ParseHeadBytes(i.Pool, buf)

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

func (i *Instance) getTable(table string) (*Table, error) {
	tb, err := i.Db.Get([]byte(table))
	if err != nil {
		log.Error().Err(err).Msg("Cannot get table")
		return nil, err
	}

	t := new(Table).Deserialize(i.Pool, i.Pool.newBuffer(tb))
	return t, nil
}

func Partial(table, col string) string {
	return fmt.Sprint(table + "." + col)
}

func (i *Instance) columnIdxMap(table, col string) (string, error) {
	hash := xxhash.Sum64String(Partial(table, col))
	if i.cache.Contains(hash) {
		d, _ := i.cache.Get(hash)
		log.Info().Msg("cache hit finding column")
		return Partial(table, d.Id), nil
	}
	t, err := i.getTable(table)
	if err != nil {
		log.Error().Err(err).Msg("Cannot get table")
	}
	num, ok := t.Map[col]

	if ok {
		str_num := strconv.Itoa(int(num))
		i.cache.Add(hash, Cacheable{Id: str_num})
		return Partial(table, str_num), nil
	}
	return "", NoColFound
}

func (i *Instance) GetColumn(table, col string) (*[][]byte, error) {
	filter, err := i.columnIdxMap(table, col)
	if err != nil {
		log.Err(err).Msg("Cannot get column")
		return nil, err
	}
	iter, err := i.Db.NewIterator(lotusdb.IteratorOptions{
		Prefix: []byte(filter),
	})
	defer iter.Close()
	if err != nil {
		log.Error().Err(err).Msg("Bad iterator with prefix")
		return nil, err
	}
	bs := [][]byte{}
	for iter.Valid() {
		bs = append(bs, iter.Value())
		iter.Next()
	}
	return &bs, nil
}
