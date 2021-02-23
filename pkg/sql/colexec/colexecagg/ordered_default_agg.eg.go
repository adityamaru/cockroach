// Code generated by execgen; DO NOT EDIT.
// Copyright 2020 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecagg

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type defaultOrderedAgg struct {
	orderedAggregateFuncBase
	fn  tree.AggregateFunc
	ctx context.Context
	// inputArgsConverter is managed by the aggregator, and this function can
	// simply call GetDatumColumn.
	inputArgsConverter *colconv.VecToDatumConverter
	resultConverter    func(tree.Datum) interface{}
	scratch            struct {
		// Note that this scratch space is shared among all aggregate function
		// instances created by the same alloc object.
		otherArgs []tree.Datum
	}
}

var _ AggregateFunc = &defaultOrderedAgg{}

func (a *defaultOrderedAgg) SetOutput(vec coldata.Vec) {
	a.orderedAggregateFuncBase.SetOutput(vec)
}

func (a *defaultOrderedAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	// Note that we only need to account for the memory of the output vector
	// and not for the intermediate results of aggregation since the aggregate
	// function itself does the latter.
	a.allocator.PerformOperation([]coldata.Vec{a.vec}, func() {
		// Capture groups to force bounds check to work. See
		// https://github.com/golang/go/issues/39756
		groups := a.groups
		if sel == nil {
			_ = groups[inputLen-1]
			for tupleIdx := 0; tupleIdx < inputLen; tupleIdx++ {
				//gcassert:bce
				if groups[tupleIdx] {
					if !a.isFirstGroup {
						res, err := a.fn.Result()
						if err != nil {
							colexecerror.ExpectedError(err)
						}
						if res == tree.DNull {
							a.nulls.SetNull(a.curIdx)
						} else {
							coldata.SetValueAt(a.vec, a.resultConverter(res), a.curIdx)
						}
						a.curIdx++
						a.fn.Reset(a.ctx)
					}
					a.isFirstGroup = false
				}
				// Note that the only function that takes no arguments is COUNT_ROWS, and
				// it has an optimized implementation, so we don't need to check whether
				// len(inputIdxs) is at least 1.
				firstArg := a.inputArgsConverter.GetDatumColumn(int(inputIdxs[0]))[tupleIdx]
				for j, colIdx := range inputIdxs[1:] {
					a.scratch.otherArgs[j] = a.inputArgsConverter.GetDatumColumn(int(colIdx))[tupleIdx]
				}
				if err := a.fn.Add(a.ctx, firstArg, a.scratch.otherArgs...); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		} else {
			// Both aggregators convert the batch "sparsely" - without
			// deselection - so converted values are at the same positions as
			// the original ones.
			for _, tupleIdx := range sel[:inputLen] {
				if groups[tupleIdx] {
					if !a.isFirstGroup {
						res, err := a.fn.Result()
						if err != nil {
							colexecerror.ExpectedError(err)
						}
						if res == tree.DNull {
							a.nulls.SetNull(a.curIdx)
						} else {
							coldata.SetValueAt(a.vec, a.resultConverter(res), a.curIdx)
						}
						a.curIdx++
						a.fn.Reset(a.ctx)
					}
					a.isFirstGroup = false
				}
				// Note that the only function that takes no arguments is COUNT_ROWS, and
				// it has an optimized implementation, so we don't need to check whether
				// len(inputIdxs) is at least 1.
				firstArg := a.inputArgsConverter.GetDatumColumn(int(inputIdxs[0]))[tupleIdx]
				for j, colIdx := range inputIdxs[1:] {
					a.scratch.otherArgs[j] = a.inputArgsConverter.GetDatumColumn(int(colIdx))[tupleIdx]
				}
				if err := a.fn.Add(a.ctx, firstArg, a.scratch.otherArgs...); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		}
	})
}

func (a *defaultOrderedAgg) Flush(outputIdx int) {
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	res, err := a.fn.Result()
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	if res == tree.DNull {
		a.nulls.SetNull(outputIdx)
	} else {
		coldata.SetValueAt(a.vec, a.resultConverter(res), outputIdx)
	}
}

func (a *defaultOrderedAgg) HandleEmptyInputScalar() {
	outputIdx := 0
	res, err := a.fn.Result()
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	if res == tree.DNull {
		a.nulls.SetNull(outputIdx)
	} else {
		coldata.SetValueAt(a.vec, a.resultConverter(res), outputIdx)
	}
}

func (a *defaultOrderedAgg) Reset() {
	a.orderedAggregateFuncBase.Reset()
	a.fn.Reset(a.ctx)
}

func newDefaultOrderedAggAlloc(
	allocator *colmem.Allocator,
	constructor execinfrapb.AggregateConstructor,
	evalCtx *tree.EvalContext,
	inputArgsConverter *colconv.VecToDatumConverter,
	numArguments int,
	constArguments tree.Datums,
	outputType *types.T,
	allocSize int64,
) *defaultOrderedAggAlloc {
	var otherArgsScratch []tree.Datum
	if numArguments > 1 {
		otherArgsScratch = make([]tree.Datum, numArguments-1)
	}
	return &defaultOrderedAggAlloc{
		aggAllocBase: aggAllocBase{
			allocator: allocator,
			allocSize: allocSize,
		},
		constructor:        constructor,
		evalCtx:            evalCtx,
		inputArgsConverter: inputArgsConverter,
		resultConverter:    colconv.GetDatumToPhysicalFn(outputType),
		otherArgsScratch:   otherArgsScratch,
		arguments:          constArguments,
	}
}

type defaultOrderedAggAlloc struct {
	aggAllocBase
	aggFuncs []defaultOrderedAgg

	constructor execinfrapb.AggregateConstructor
	evalCtx     *tree.EvalContext
	// inputArgsConverter is a converter from coldata.Vecs to tree.Datums that
	// is shared among all aggregate functions and is managed by the aggregator
	// (meaning that the aggregator operator is responsible for calling
	// ConvertBatch method).
	inputArgsConverter *colconv.VecToDatumConverter
	resultConverter    func(tree.Datum) interface{}
	// otherArgsScratch is the scratch space for arguments other than first one
	// that is shared among all aggregate functions created by this alloc. Such
	// sharing is acceptable since the aggregators run in a single goroutine
	// and they process functions one at a time.
	otherArgsScratch []tree.Datum
	// arguments is the list of constant (non-aggregated) arguments to the
	// aggregate, for instance, the separator in string_agg.
	arguments tree.Datums
	// returnedFns stores the references to all aggregate functions that have
	// been returned by this alloc. Such tracking is necessary since
	// row-execution aggregate functions need to be closed (unlike optimized
	// vectorized equivalents), and the alloc object is a convenient way to do
	// so.
	// TODO(yuzefovich): it might make sense to introduce Close method into
	// colexecagg.AggregateFunc interface (which would be a noop for all optimized
	// functions) and move the responsibility of closing to the aggregators
	// because they already have references to all aggregate functions.
	returnedFns []*defaultOrderedAgg
}

var _ aggregateFuncAlloc = &defaultOrderedAggAlloc{}
var _ colexecbase.Closer = &defaultOrderedAggAlloc{}

const sizeOfDefaultOrderedAgg = int64(unsafe.Sizeof(defaultOrderedAgg{}))
const defaultOrderedAggSliceOverhead = int64(unsafe.Sizeof([]defaultOrderedAggAlloc{}))

func (a *defaultOrderedAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(defaultOrderedAggSliceOverhead + sizeOfDefaultOrderedAgg*a.allocSize)
		a.aggFuncs = make([]defaultOrderedAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	*f = defaultOrderedAgg{
		fn:                 a.constructor(a.evalCtx, a.arguments),
		ctx:                a.evalCtx.Context,
		inputArgsConverter: a.inputArgsConverter,
		resultConverter:    a.resultConverter,
	}
	f.allocator = a.allocator
	f.scratch.otherArgs = a.otherArgsScratch
	a.allocator.AdjustMemoryUsage(f.fn.Size())
	a.aggFuncs = a.aggFuncs[1:]
	a.returnedFns = append(a.returnedFns, f)
	return f
}

func (a *defaultOrderedAggAlloc) Close(ctx context.Context) error {
	for _, fn := range a.returnedFns {
		fn.fn.Close(ctx)
	}
	a.returnedFns = nil
	return nil
}
