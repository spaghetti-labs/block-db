package buffer

import "encoding/binary"

type Buffer []byte

func (b *Buffer) ReadUint64() uint64 {
	v := binary.BigEndian.Uint64(*b)
	*b = (*b)[8:]
	return v
}

func (b *Buffer) WriteUint64(v uint64) {
	binary.BigEndian.PutUint64(*b, v)
	*b = (*b)[8:]
}
