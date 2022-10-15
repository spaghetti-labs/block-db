package chain

type Chain[B any] interface {
	Close()
	Length() uint64
	WriteBlocks(index uint64, blocks []B) error
	ReadBlocks(index uint64, blocks []B) error
	ReadBlock(index uint64, block *B) error
}
