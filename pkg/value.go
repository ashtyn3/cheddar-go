package cheddar

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"strconv"

	"go.uber.org/zap"
)

type CCompatible interface {
	CString | CInt64 | CFloat64 | CBool | CKey
}

// type RawCCompatible interface {
// 	string | int | float64 | bool
// }

const (
	NULL = iota
	STRING
	INT64
	FLOAT64
	BOOL
	KEY
)

type Head struct {
	Size int64
	Kind int8
}

type Serial interface {
	Serialize() []byte
}
type Deserial[T CCompatible] interface {
	Deserialize(bytes.Buffer) T
}

type CString string
type CInt64 int64
type CFloat64 float64
type CBool bool
type CKey struct {
	Table []byte
	Col   uint64
	Id    []byte
}

func FullSerial(kind int8, body []byte) []byte {
	// size := V(len(body)).(CInt64).Serialize()
	size := make([]byte, 8)
	binary.LittleEndian.PutUint64(size, uint64(len(body)))

	k := byte(kind)

	data := []byte{}
	data = append(data, k)
	data = append(data, size...)
	data = append(data, byte('|'))
	data = append(data, body...)

	return data
}
func (cs CString) Serialize() []byte {
	return FullSerial(STRING, []byte(cs))
}

func ParseKeyBytes(r []byte) CKey {
	parts := bytes.Split(r, []byte("."))

	col, _ := strconv.Atoi(string(parts[1]))
	return CKey{
		Table: parts[0],
		Col:   uint64(col),
		Id:    parts[2],
	}
}
func ParseHeadBytes(r *bytes.Buffer) Head {
	bs, _ := r.ReadBytes(byte('|'))
	bs = bytes.Trim(bs, "|")
	headBuffer := bytes.NewBuffer(bs)

	k, _ := headBuffer.ReadByte()

	num := binary.LittleEndian.Uint64(headBuffer.Bytes())
	return Head{Kind: int8(k), Size: int64(num)}
}
func (cs *CString) Deserialize(r *bytes.Buffer) (CString, Head) {

	h := ParseHeadBytes(r)
	body := make([]byte, h.Size)
	n, err := r.Read(body)
	if err != nil {
		zap.L().Error("could not deserialize", zap.Error(err))
	}

	return CString(string(body)), Head{
		Size: int64(n),
		Kind: STRING,
	}
}

func (cs CInt64) Serialize() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(cs))

	return FullSerial(INT64, b)
}

func (cs CInt64) Deserialize(r *bytes.Buffer) (CInt64, Head) {
	h := ParseHeadBytes(r)
	body := make([]byte, h.Size)
	r.Read(body)

	num := binary.LittleEndian.Uint64(body)

	return CInt64(int64(num)), Head{
		Size: 8,
		Kind: INT64,
	}
}

func (cs CFloat64) Serialize() []byte {
	b := make([]byte, 8)
	u := math.Float64bits(float64(cs))
	binary.LittleEndian.PutUint64(b, u)

	return FullSerial(FLOAT64, b)
}

func (cs CFloat64) Deserialize(r *bytes.Buffer) (CFloat64, Head) {
	h := ParseHeadBytes(r)
	body := make([]byte, h.Size)
	r.Read(body)

	num := math.Float64frombits(binary.LittleEndian.Uint64(body))

	return CFloat64(float64(num)), Head{
		Size: h.Size,
		Kind: h.Kind,
	}
}

func (cs CBool) Serialize() []byte {
	if cs == true {
		return FullSerial(BOOL, []byte{1})
	}
	return FullSerial(BOOL, []byte{0})
}

func (cs CBool) Deserialize(r *bytes.Buffer) (CBool, Head) {
	ParseHeadBytes(r)
	b, _ := r.ReadByte()
	h := Head{
		Size: 1,
		Kind: BOOL,
	}

	if b == 0 {
		return false, h
	}
	return true, h
}

func V(v interface{}) Serial {
	switch v := v.(type) {
	case int:
		{
			data := int64(v)
			return CInt64(data)
		}
	case float64:
		{
			return CFloat64(v)
		}
	case string:
		{
			return CString(v)
		}
	case bool:
		{
			return CBool(v)
		}
	default:
		{
			zap.L().Fatal("broken type")
		}
	}
	return nil
}

func T(v uint8) reflect.Type {
	switch v {
	case STRING:
		{
			return reflect.TypeOf(CString(""))
		}
	case INT64:
		{
			return reflect.TypeOf(CInt64(0))
		}
	case FLOAT64:
		{
			return reflect.TypeOf(CFloat64(0))
		}
	case BOOL:
		{
			return reflect.TypeOf(CBool(false))
		}
	}
	panic("unreachable")
}
