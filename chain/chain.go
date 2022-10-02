package chain

type Chain[B any] interface {
	Close()
	Length() (uint, error)
	WriteBlocks(index uint, blocks []B) error
	ReadBlocks(index uint, blocks []B) error
	ReadBlock(index uint, block *B) error
}
