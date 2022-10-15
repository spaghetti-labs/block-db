package chain

import (
	"errors"
	"os"
	"path"
	"sync"

	"github.com/spaghetti-labs/block-db/buffer"
)

type VariableBlockController[B any] struct {
	Size      func(block *B) uint64
	Marshal   func(block *B, buffer *buffer.Buffer)
	Unmarshal func(block *B, buffer *buffer.Buffer)
}

type VariableChain[B any] struct {
	controller  VariableBlockController[B]
	dataFile    *os.File
	offsetChain *FixedChain[uint64]
	mutex       sync.Mutex
}

var uint64Controller = FixedBlockController[uint64]{
	Size: 8,
	Marshal: func(block *uint64, buffer *buffer.Buffer) {
		buffer.WriteUint64(*block)
	},
	Unmarshal: func(block *uint64, buffer *buffer.Buffer) {
		*block = buffer.ReadUint64()
	},
}

func OpenVariableChain[B any](controller VariableBlockController[B], dirPath string) (*VariableChain[B], error) {
	err := os.MkdirAll(dirPath, 0700)
	if err != nil {
		return nil, err
	}

	offsetChain, err := OpenFixedChain(uint64Controller, path.Join(dirPath, "offset.bin"))
	if err != nil {
		return nil, err
	}

	dataFile, err := os.OpenFile(
		path.Join(dirPath, "data.bin"),
		os.O_CREATE|os.O_RDWR,
		0700,
	)
	if err != nil {
		offsetChain.Close()
		return nil, err
	}

	chain := VariableChain[B]{
		controller:  controller,
		dataFile:    dataFile,
		offsetChain: offsetChain,
	}

	return &chain, nil
}

func (chain *VariableChain[B]) Close() {
	chain.dataFile.Close()
	chain.offsetChain.Close()
}

func (chain *VariableChain[B]) Length() uint64 {
	return chain.offsetChain.Length()
}

func (chain *VariableChain[B]) readOffset(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	var offset uint64
	err := chain.offsetChain.ReadBlock(index-1, &offset)
	return offset, err
}

func (chain *VariableChain[B]) readEndOffset(index uint64) (uint64, error) {
	var offset uint64
	err := chain.offsetChain.ReadBlock(index, &offset)
	return offset, err
}

func (chain *VariableChain[B]) readOffsets(index uint64, count uint64) (startOffset uint64, endOffset uint64, err error) {
	if count == 0 {
		err = errors.New("count must be > 0")
		return
	}

	if index != 0 {
		err = chain.offsetChain.ReadBlock(index-1, &startOffset)
		if err != nil {
			return
		}
	}

	err = chain.offsetChain.ReadBlock(index+count-1, &endOffset)
	if err != nil {
		return
	}

	if endOffset < startOffset {
		err = errors.New("inconsistent offsets")
		return
	}

	return
}

func (chain *VariableChain[B]) WriteBlocks(index uint64, blocks []B) error {
	chain.mutex.Lock()
	defer chain.mutex.Unlock()

	offset, err := chain.readOffset(index)
	if err != nil {
		return err
	}

	count := uint(len(blocks))

	var totalSize uint64
	endOffsets := make([]uint64, count)
	for i := range blocks {
		totalSize += chain.controller.Size(&blocks[i])
		endOffsets[i] = offset + uint64(totalSize)
	}
	bytes := make([]byte, totalSize)
	buffer := buffer.Buffer(bytes)
	for i := range blocks {
		chain.controller.Marshal(&blocks[i], &buffer)
	}
	if len(buffer) != 0 {
		return errors.New("inconsistent block bytes")
	}
	_, err = chain.dataFile.WriteAt(bytes, int64(offset))
	if err != nil {
		return err
	}

	err = chain.offsetChain.WriteBlocks(index, endOffsets)

	return err
}

func (chain *VariableChain[B]) ReadBlocks(index uint64, blocks []B) error {
	chain.mutex.Lock()
	defer chain.mutex.Unlock()

	count := uint64(len(blocks))

	startOffset, endOffset, err := chain.readOffsets(index, count)
	if err != nil {
		return err
	}

	totalSize := endOffset - startOffset

	bytes := make([]byte, totalSize)
	buffer := buffer.Buffer(bytes)
	_, err = chain.dataFile.ReadAt(bytes, int64(startOffset))
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

func (chain *VariableChain[B]) ReadBlock(index uint64, block *B) error {
	chain.mutex.Lock()
	defer chain.mutex.Unlock()

	startOffset, endOffset, err := chain.readOffsets(index, 1)
	if err != nil {
		return err
	}

	blockSize := endOffset - startOffset

	bytes := make([]byte, blockSize)
	buffer := buffer.Buffer(bytes)
	_, err = chain.dataFile.ReadAt(bytes, int64(startOffset))
	if err != nil {
		return err
	}

	chain.controller.Unmarshal(block, &buffer)
	if len(buffer) != 0 {
		return errors.New("inconsistent block bytes")
	}

	return nil
}
