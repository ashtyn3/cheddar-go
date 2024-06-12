package cheddar

import (
	"bytes"
	"fmt"

	"go.uber.org/zap"
)

type Table struct {
	Cols  []Column
	Map   map[string]uint16
	cap   int
	Name  CString
	Keyed CBool
}

func (t *Table) New(name string, size uint16) *Table {

	t.Name = CString(name)
	t.Cols = make([]Column, size)
	t.cap = -1
	t.Map = make(map[string]uint16, size)

	return t
}

func (t *Table) Column(c *Column) *Table {
	t.cap += 1
	if t.cap >= len(t.Cols) {
		zap.L().Fatal("Bad column number")
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

func (t *Table) Deserialize(r *bytes.Buffer) *Table {

	size, _ := new(CInt64).Deserialize(r)
	keyed, _ := new(CBool).Deserialize(r)
	t.Keyed = keyed
	name, _ := new(CString).Deserialize(r)
	t.Name = name
	t.Map = make(map[string]uint16, size)
	cols := []Column{}
	fmt.Println(r.Bytes())

	for r.Available() != 0 {
		b, err := r.ReadBytes(byte('\n'))
		if err != nil {
			break
		}
		c := *new(Column).Deserialize(bytes.NewBuffer(b))
		c.Index = uint16(len(cols))
		t.Map[string(c.Name)] = c.Index
		cols = append(cols, c)
	}
	t.Cols = cols
	return t
}
