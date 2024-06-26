package cheddar

import (
	"strconv"
)

type RowSegment struct {
	Key   CKey
	Head  Head
	Value []byte
	pool  *pool
}

func (rs *RowSegment) NewSegment(p *pool) {
	rs.pool = p
}

func (rs *RowSegment) MarshalJSON() ([]byte, error) {
	switch rs.Head.Kind {
	case STRING:
		{
			v, _ := new(CString).Deserialize(rs.pool, rs.pool.newBuffer(rs.Value))
			return []byte(v), nil
		}
	case INT64:
		{
			v, _ := new(CInt64).Deserialize(rs.pool, rs.pool.newBuffer(rs.Value))
			return []byte(strconv.Itoa(int(v))), nil
		}
	case FLOAT64:
		{
			v, _ := new(CFloat64).Deserialize(rs.pool, rs.pool.newBuffer(rs.Value))
			return []byte(strconv.FormatFloat(float64(v), 'E', -1, 64)), nil
		}
	case BOOL:
		{
			v, _ := new(CBool).Deserialize(rs.pool, rs.pool.newBuffer(rs.Value))
			return []byte(strconv.FormatBool(bool(v))), nil
		}
	}
	panic("unreachable")
}
