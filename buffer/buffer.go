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

func (b *Buffer) ReadUint32() uint32 {
	v := binary.BigEndian.Uint32(*b)
	*b = (*b)[4:]
	return v
}

func (b *Buffer) WriteUint32(v uint32) {
	binary.BigEndian.PutUint32(*b, v)
	*b = (*b)[4:]
}

func (b *Buffer) ReadUint16() uint16 {
	v := binary.BigEndian.Uint16(*b)
	*b = (*b)[2:]
	return v
}

func (b *Buffer) WriteUint16(v uint16) {
	binary.BigEndian.PutUint16(*b, v)
	*b = (*b)[2:]
}

func (b *Buffer) ReadUint8() uint8 {
	var v uint8
	v = (*b)[0]
	*b = (*b)[1:]
	return v
}

func (b *Buffer) WriteUint8(v uint8) {
	(*b)[0] = v
	*b = (*b)[1:]
}

func (b *Buffer) ReadBytes(target []byte) {
	copy(target, *b)
	*b = (*b)[len(target):]
}

func (b *Buffer) ReadNewBytes(length uint) []byte {
	bytes := make([]byte, length)
	b.ReadBytes(bytes)
	return bytes
}

func (b *Buffer) Slice(length uint) []byte {
	slice := (*b)[:length]
	*b = (*b)[length:]
	return slice
}

func (b *Buffer) WriteBytes(source []byte) {
	copy(*b, source)
	*b = (*b)[len(source):]
}
