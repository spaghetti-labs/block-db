package chain

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/spaghetti-labs/block-db/buffer"
)

type FixedBlockController[B any] struct {
	Size      uint64
	Marshal   func(block *B, buffer *buffer.Buffer)
	Unmarshal func(block *B, buffer *buffer.Buffer)
}

type FixedChain[B any] struct {
	controller FixedBlockController[B]
	file       *os.File
	mutex      sync.Mutex

	// Cache
	length uint64
}

func OpenFixedChain[B any](controller FixedBlockController[B], filePath string) (*FixedChain[B], error) {
	file, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR,
		0700,
	)
	if err != nil {
		return nil, err
	}

	chain := FixedChain[B]{
		controller: controller,
		file:       file,
	}

	chain.length, err = chain.readLength()
	if err != nil {
		chain.Close()
		return nil, err
	}

	return &chain, nil
}

func (chain *FixedChain[B]) Close() {
	chain.file.Close()
}

func (chain *FixedChain[B]) readLength() (uint64, error) {
	blockSize := chain.controller.Size
	stat, err := chain.file.Stat()
	if err != nil {
		return 0, err
	}
	length := uint64(stat.Size()) / blockSize
	return length, nil
}

func (chain *FixedChain[B]) Length() uint64 {
	return atomic.LoadUint64(&chain.length)
}

func (chain *FixedChain[B]) WriteBlocks(index uint64, blocks []B) error {
	chain.mutex.Lock()
	defer chain.mutex.Unlock()

	length := chain.Length()
	if index > length {
		return errors.New("writing with gap")
	}

	blockSize := chain.controller.Size
	count := uint64(len(blocks))
	totalSize := blockSize * count
	bytes := make([]byte, totalSize)
	buffer := buffer.Buffer(bytes)
	for i := range blocks {
		chain.controller.Marshal(&blocks[i], &buffer)
	}
	if len(buffer) != 0 {
		return errors.New("inconsistent block bytes")
	}
	offset := blockSize * index
	_, err := chain.file.WriteAt(bytes, int64(offset))

	end := index + count
	if end > length {
		atomic.StoreUint64(&chain.length, end)
	}

	return err
}

func (chain *FixedChain[B]) ReadBlocks(index uint64, blocks []B) error {
	blockSize := chain.controller.Size
	count := uint64(len(blocks))

	length := chain.Length()

	end := index + count
	if end > length {
		return errors.New("reading with gap")
	}

	totalSize := blockSize * uint64(count)
	bytes := make([]byte, totalSize)
	buffer := buffer.Buffer(bytes)
	offset := blockSize * index
	_, err := chain.file.ReadAt(bytes, int64(offset))
	if err != nil {
		return err
	}
	for i := range blocks {
		chain.controller.Unmarshal(&blocks[i], &buffer)
	}
	if len(buffer) != 0 {
		return errors.New("inconsistent block bytes")
	}
	return nil
}

func (chain *FixedChain[B]) ReadBlock(index uint64, block *B) error {
	blockSize := chain.controller.Size
	bytes := make([]byte, blockSize)
	buffer := buffer.Buffer(bytes)
	offset := blockSize * index
	_, err := chain.file.ReadAt(bytes, int64(offset))
	if err != nil {
		return err
	}
	chain.controller.Unmarshal(block, &buffer)
	if len(buffer) != 0 {
		return errors.New("inconsistent block bytes")
	}
	return nil
}
