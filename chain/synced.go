package chain

import "errors"

type Synchroniser[D any, S any] func(carry *S, data []D, results []S)

type SyncedChain[D any, S any] struct {
	dataChain    Chain[D]
	synchroniser Synchroniser[D, S]
	syncChain    *FixedChain[S]
}

func NewSyncedChain[D any, S any](
	dataChain Chain[D],
	synchroniser Synchroniser[D, S],
	syncChain *FixedChain[S],
) *SyncedChain[D, S] {
	chain := SyncedChain[D, S]{
		dataChain:    dataChain,
		synchroniser: synchroniser,
		syncChain:    syncChain,
	}
	return &chain
}

func OpenSyncedChain[D any, S any](
	dataChain Chain[D],
	syncController FixedBlockController[S],
	synchroniser Synchroniser[D, S],
	filePath string,
) (*SyncedChain[D, S], error) {
	syncChain, err := OpenFixedChain(
		syncController,
		filePath,
	)
	if err != nil {
		return nil, err
	}

	return NewSyncedChain(
		dataChain,
		synchroniser,
		syncChain,
	), nil
}

func (chain *SyncedChain[D, S]) Close() {
	chain.syncChain.Close()
}

func (chain *SyncedChain[D, S]) Sync() error {
	var err error

	dataLength := chain.dataChain.Length()

	length := chain.syncChain.Length()

	if dataLength < length {
		return errors.New("inconsistent chain lengths")
	}

	diffLength := dataLength - length
	if diffLength == 0 {
		return nil
	}

	var carry *S
	if length > 0 {
		carry = new(S)
		err = chain.syncChain.ReadBlock(length-1, carry)
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
	syncs := make([]S, buffSize)

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

		chain.synchroniser(carry, data[:batchSize], syncs[:batchSize])

		err = chain.syncChain.WriteBlocks(index, syncs[:batchSize])
		if err != nil {
			return err
		}

		carryVal := syncs[batchSize-1]
		carry = &carryVal
		i += batchSize
	}

	return nil
}

func (chain *SyncedChain[D, S]) ReadSync(index uint64, result *S) error {
	err := chain.syncChain.ReadBlock(index, result)
	if err != nil {
		return err
	}

	return nil
}
