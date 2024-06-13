package cheddar

import (
	"bytes"

	"github.com/rs/zerolog/log"
)

type Table struct {
	Cols  []Column
	Map   map[string]uint16
	cap   int
	Name  CString
	Keyed CBool
	pool  *pool
}

func (t *Table) New(p *pool, name string, size uint16) *Table {

	t.Name = CString(name)
	t.Cols = make([]Column, size)
	t.cap = -1
	t.Map = make(map[string]uint16, size)
	t.pool = p

	return t
}

func (t *Table) Column(c *Column) *Table {
	t.cap += 1
	if t.cap >= len(t.Cols) {
		log.Fatal().Msg("Bad column number")
	}
	c.Index = uint16(t.cap)
	t.Cols[t.cap] = *c
	t.Map[string(c.Name)] = uint16(t.cap)
	return t
}

func (t *Table) Serialize() []byte {
	serial_name := t.Name.Serialize()
	data := []byte{}
	data = append(data, V(len(t.Cols)).Serialize()...)
	data = append(data, t.Keyed.Serialize()...)
	data = append(data, serial_name...)

	for _, el := range t.Cols {
		data = append(data, el.Serialize()...)
		data = append(data, byte('\n'))
	}

	return data
}

func (t *Table) Deserialize(p *pool, r *bytes.Buffer) *Table {
	t.pool = p
	size, _ := new(CInt64).Deserialize(t.pool, r)
	keyed, _ := new(CBool).Deserialize(t.pool, r)
	t.Keyed = keyed
	name, _ := new(CString).Deserialize(t.pool, r)
	t.Name = name
	t.Map = make(map[string]uint16, size)
	cols := []Column{}

	for r.Available() != 0 {
		b, err := r.ReadBytes(byte('\n'))
		if err != nil {
			break

		}
		c := *new(Column).Deserialize(t.pool, t.pool.newBuffer(b))
		c.Index = uint16(len(cols))
		t.Map[string(c.Name)] = c.Index
		cols = append(cols, c)
	}
	t.Cols = cols
	t.pool.p.Put(r)
	return t
}
