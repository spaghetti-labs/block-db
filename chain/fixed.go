package chain

import (
	"errors"
	"os"
	"sync"

	"github.com/spaghetti-labs/block-db/buffer"
)

type FixedBlockController[B any] struct {
	Size      uint
	Marshal   func(block *B, buffer *buffer.Buffer)
	Unmarshal func(block *B, buffer *buffer.Buffer)
}

type FixedChain[B any] struct {
	controller FixedBlockController[B]
	file       *os.File
	mutex      sync.Mutex
}

func NewFixedChain[B any](controller FixedBlockController[B], file *os.File) *FixedChain[B] {
	chain := FixedChain[B]{
		controller: controller,
		file:       file,
	}
	return &chain
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
	return NewFixedChain(controller, file), nil
}

func (chain *FixedChain[B]) Close() {
	chain.file.Close()
}

func (chain *FixedChain[B]) Length() (uint, error) {
	blockSize := chain.controller.Size
	stat, err := chain.file.Stat()
	if err != nil {
		return 0, err
	}
	length := uint(stat.Size()) / blockSize
	return length, nil
}

func (chain *FixedChain[B]) WriteBlocks(index uint, blocks []B) error {
	chain.mutex.Lock()
	defer chain.mutex.Unlock()
	blockSize := chain.controller.Size
	count := len(blocks)
	totalSize := blockSize * uint(count)
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
	return err
}

func (chain *FixedChain[B]) ReadBlocks(index uint, blocks []B) error {
	blockSize := chain.controller.Size
	count := len(blocks)
	totalSize := blockSize * uint(count)
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

func (chain *FixedChain[B]) ReadBlock(index uint, block *B) error {
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
