package chain

import "errors"

type Accumulator[D any, C any] struct {
	Accumulate func(carry *C, data []D, results []C)
	Difference func(from *C, to *C, result *C)
}

type AccumulatedChain[D any, C any] struct {
	dataChain       Chain[D]
	accumulator     Accumulator[D, C]
	cumulativeChain *FixedChain[C]
}

func NewAccumulatedChain[D any, C any](
	dataChain Chain[D],
	accumulator Accumulator[D, C],
	cumulativeChain *FixedChain[C],
) *AccumulatedChain[D, C] {
	chain := AccumulatedChain[D, C]{
		dataChain:       dataChain,
		accumulator:     accumulator,
		cumulativeChain: cumulativeChain,
	}
	return &chain
}

func OpenAccumulatedChain[D any, C any](
	dataChain Chain[D],
	cumulativeController FixedBlockController[C],
	accumulator Accumulator[D, C],
	filePath string,
) (*AccumulatedChain[D, C], error) {
	cumulativeChain, err := OpenFixedChain(
		cumulativeController,
		filePath,
	)
	if err != nil {
		return nil, err
	}

	return NewAccumulatedChain(
		dataChain,
		accumulator,
		cumulativeChain,
	), nil
}

func (chain *AccumulatedChain[D, C]) Close() {
	chain.cumulativeChain.Close()
}

func (chain *AccumulatedChain[D, C]) Sync() error {
	dataLength, err := chain.dataChain.Length()
	if err != nil {
		return err
	}

	length, err := chain.cumulativeChain.Length()
	if err != nil {
		return err
	}

	if dataLength < length {
		return errors.New("inconsistent chain lengths")
	}

	diffLength := dataLength - length
	if diffLength == 0 {
		return nil
	}

	var carry *C
	if length > 0 {
		carry = new(C)
		err = chain.cumulativeChain.ReadBlock(length-1, carry)
		if err != nil {
			return err
		}
	}

	const maxBatchSize = uint(32768)
	var buffSize uint
	if diffLength > maxBatchSize {
		buffSize = maxBatchSize
	} else {
		buffSize = diffLength
	}

	data := make([]D, buffSize)
	cums := make([]C, buffSize)

	for i := uint(0); i < diffLength; {
		var batchSize uint
		remaining := diffLength - i
		if remaining > maxBatchSize {
			batchSize = maxBatchSize
		} else {
			batchSize = remaining
		}

		index := length + i

		err = chain.dataChain.ReadBlocks(index, data[:batchSize])
		if err != nil {
			return err
		}

		chain.accumulator.Accumulate(carry, data[:batchSize], cums[:batchSize])

		err = chain.cumulativeChain.WriteBlocks(index, cums[:batchSize])
		if err != nil {
			return err
		}

		carryVal := cums[batchSize-1]
		carry = &carryVal
		i += batchSize
	}

	return nil
}

func (chain *AccumulatedChain[D, C]) Accumulate(index uint, length uint, result *C) error {
	if length == 0 {
		return errors.New("zero length accumulation")
	}

	var tail C
	err := chain.cumulativeChain.ReadBlock(index+length-1, &tail)
	if err != nil {
		return err
	}

	if index == 0 {
		*result = tail
		return nil
	}

	var head C
	err = chain.cumulativeChain.ReadBlock(index-1, &head)
	if err != nil {
		return err
	}

	chain.accumulator.Difference(&head, &tail, result)
	return nil
}
