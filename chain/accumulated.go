package chain

import "errors"

type Accumulator[D any, E any, C any] struct {
	Edge       func(carry *E, data []D, edges []E)
	Accumulate func(from *E, to *E, cumulative *C)
}

type AccumulatedChain[D any, E any, C any] struct {
	dataChain   Chain[D]
	accumulator Accumulator[D, E, C]
	edgeChain   *FixedChain[E]
}

func NewAccumulatedChain[D any, E any, C any](
	dataChain Chain[D],
	accumulator Accumulator[D, E, C],
	edgeChain *FixedChain[E],
) *AccumulatedChain[D, E, C] {
	chain := AccumulatedChain[D, E, C]{
		dataChain:   dataChain,
		accumulator: accumulator,
		edgeChain:   edgeChain,
	}
	return &chain
}

func OpenAccumulatedChain[D any, E any, C any](
	dataChain Chain[D],
	edgeController FixedBlockController[E],
	accumulator Accumulator[D, E, C],
	filePath string,
) (*AccumulatedChain[D, E, C], error) {
	edgeChain, err := OpenFixedChain(
		edgeController,
		filePath,
	)
	if err != nil {
		return nil, err
	}

	return NewAccumulatedChain(
		dataChain,
		accumulator,
		edgeChain,
	), nil
}

func (chain *AccumulatedChain[D, E, C]) Close() {
	chain.edgeChain.Close()
}

func (chain *AccumulatedChain[D, E, C]) Sync() error {
	var err error

	dataLength := chain.dataChain.Length()

	length := chain.edgeChain.Length()

	if dataLength < length {
		return errors.New("inconsistent chain lengths")
	}

	diffLength := dataLength - length
	if diffLength == 0 {
		return nil
	}

	var carry *E
	if length > 0 {
		carry = new(E)
		err = chain.edgeChain.ReadBlock(length-1, carry)
		if err != nil {
			return err
		}
	}

	const maxBatchSize = uint64(32768)
	var buffSize uint64
	if diffLength > maxBatchSize {
		buffSize = maxBatchSize
	} else {
		buffSize = diffLength
	}

	data := make([]D, buffSize)
	edges := make([]E, buffSize)

	for i := uint64(0); i < diffLength; {
		var batchSize uint64
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

		chain.accumulator.Edge(carry, data[:batchSize], edges[:batchSize])

		err = chain.edgeChain.WriteBlocks(index, edges[:batchSize])
		if err != nil {
			return err
		}

		carryVal := edges[batchSize-1]
		carry = &carryVal
		i += batchSize
	}

	return nil
}

func (chain *AccumulatedChain[D, E, C]) ReadEdgeSeries(indices []uint64, edges []E, cumulatives []C) error {
	length := len(indices)

	if length < 2 {
		return errors.New("accumulation series must have at least 2 indices")
	}

	if length != len(edges) {
		return errors.New("edges length must be equal to indices length")
	}

	if length-1 != len(cumulatives) {
		return errors.New("cumulatives length must be 1 less than indices length")
	}

	for i := 0; i < length; i++ {
		index := indices[i]
		if i > 0 && index <= indices[i-1] {
			return errors.New("indices must be in ascending order")
		}
		err := chain.edgeChain.ReadBlock(index, &edges[i])
		if err != nil {
			return err
		}
	}

	for i := 0; i < length-1; i++ {
		from := &edges[i]
		to := &edges[i+1]
		chain.accumulator.Accumulate(from, to, &cumulatives[i])
	}

	return nil
}

func (chain *AccumulatedChain[D, E, C]) ReadEdge(index uint64, result *E) error {
	err := chain.edgeChain.ReadBlock(index, result)
	if err != nil {
		return err
	}

	return nil
}
