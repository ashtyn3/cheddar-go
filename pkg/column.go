package cheddar

import (
	"bytes"
	"encoding/binary"
	"reflect"
)

type Column struct {
	Index      uint16
	NotNull    CBool
	MaxSize    CInt64
	MinSize    CInt64
	HasDefault bool
	Default    Serial
	IsPrimary  CBool
	Kind       byte
	Name       CString
}

func (c *Column) New(n CString, k byte) *Column {
	c.Name = n
	c.Kind = k

	return c
}

// TODO: Make static allocation not dynamic
func (c *Column) Serialize() []byte {
	data := []byte{}
	d := make([]byte, 2)
	binary.LittleEndian.PutUint16(d, c.Index)
	data = append(data, d...)
	data = append(data, c.IsPrimary.Serialize()...)
	data = append(data, c.NotNull.Serialize()...)
	data = append(data, c.Kind)
	data = append(data, c.Name.Serialize()...)

	if c.HasDefault {
		def := c.Default.Serialize()
		data = append(data, 1)
		data = append(data, def...)
	} else {
		data = append(data, 0)
	}
	data = append(data, c.MaxSize.Serialize()...)
	data = append(data, c.MinSize.Serialize()...)

	return data
}
func (c *Column) Deserialize(r *bytes.Buffer) *Column {
	idx_bytes := make([]byte, 2)
	r.Read(idx_bytes)
	c.Index = binary.LittleEndian.Uint16(idx_bytes)
	prim, _ := new(CBool).Deserialize(r)
	c.IsPrimary = prim

	NotNull, _ := new(CBool).Deserialize(r)
	c.NotNull = NotNull

	k, _ := r.ReadByte()
	c.Kind = k
	name, _ := new(CString).Deserialize(r)
	c.Name = name

	hasDef, _ := r.ReadByte()

	if hasDef == 1 {
		c.Default = reflect.New(T(c.Kind)).Interface().(Serial)
	}
	max, _ := new(CInt64).Deserialize(r)
	c.MaxSize = max

	min, _ := new(CInt64).Deserialize(r)
	c.MinSize = min
	return c
}
