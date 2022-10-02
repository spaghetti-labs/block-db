package chain

import (
	"errors"
	"fmt"
	"os"
	"path"
)

const levelBits uint = 7
const levelSize uint = 1 << levelBits

type Aggregator[D any, A any] func(carry []A, data []D, result *A)

type AggregatedChain[D any, A any] struct {
	dataChain         Chain[D]
	aggregator        Aggregator[D, A]
	aggregationChains []*FixedChain[A]
}

func NewAggregatedChain[D any, A any](
	dataChain Chain[D],
	aggregator Aggregator[D, A],
	aggregationChains []*FixedChain[A],
) *AggregatedChain[D, A] {
	chain := AggregatedChain[D, A]{
		dataChain:         dataChain,
		aggregator:        aggregator,
		aggregationChains: aggregationChains,
	}
	return &chain
}

func OpenAggregatedChain[D any, A any](
	dataChain Chain[D],
	aggregationController FixedBlockController[A],
	aggregator Aggregator[D, A],
	aggregationLevel uint8,
	dirPath string,
) (*AggregatedChain[D, A], error) {
	err := os.MkdirAll(dirPath, 0700)
	if err != nil {
		return nil, err
	}

	aggChains := make([]*FixedChain[A], aggregationLevel)
	for i := range aggChains {
		levelFactor := 1 << ((i + 1) * int(levelBits))
		aggChains[i], err = OpenFixedChain(
			aggregationController,
			path.Join(dirPath, fmt.Sprintf("%d.bin", levelFactor)),
		)
		if err != nil {
			for j := 0; j < i; j++ {
				aggChains[j].Close()
			}
			return nil, err
		}
	}

	return NewAggregatedChain(
		dataChain,
		aggregator,
		aggChains,
	), nil
}

func (chain *AggregatedChain[D, A]) Close() {
	for _, aggChain := range chain.aggregationChains {
		aggChain.Close()
	}
}

func (chain *AggregatedChain[D, A]) Sync() error {
	dataLength, err := chain.dataChain.Length()
	if err != nil {
		return err
	}

	deeperLen := dataLength
	shiftBits := levelBits
	for level, aggChain := range chain.aggregationChains {
		length, err := aggChain.Length()
		if err != nil {
			return err
		}
		availableLen := deeperLen >> levelBits
		if length > availableLen {
			return errors.New("inconsistent aggregation level lengths")
		}
		var aggs = make([]A, availableLen-length)
		j := 0
		for i := length; i < availableLen; i++ {
			var carry []A
			var data []D
			if level == 0 {
				data = make([]D, levelSize)
				err = chain.dataChain.ReadBlocks(i<<shiftBits, data)
				if err != nil {
					return err
				}
			} else {
				carry = make([]A, levelSize)
				err = chain.aggregationChains[level-1].ReadBlocks(i<<levelBits, carry)
				if err != nil {
					return err
				}
			}
			chain.aggregator(carry, data, &aggs[j])
			j++
		}
		err = aggChain.WriteBlocks(length, aggs)
		if err != nil {
			return err
		}
		deeperLen = availableLen
		shiftBits = shiftBits + levelBits
	}
	return nil
}
