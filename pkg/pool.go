package cheddar

import (
	"bytes"
	"sync"
)

type pool struct {
	p *sync.Pool
}

func (p *pool) New() *pool {

	p.p = &sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}

	return p
}
func (p *pool) newBuffer(data []byte) *bytes.Buffer {
	b := p.p.Get().(*bytes.Buffer)
	b.Truncate(0)
	b.Write(data)

	return b

}
