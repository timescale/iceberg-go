// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package deletes

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"golang.org/x/sync/errgroup"
)

// EqualityDeleteFilter filters data records against equality delete files.
// Rows matching any delete record (on the equality columns) are excluded.
type EqualityDeleteFilter struct {
	deleteRecords []arrow.Record
	fieldIDs      []int
	schema        *iceberg.Schema
	mem           memory.Allocator
}

// NewEqualityDeleteFilter creates a filter from equality delete files.
func NewEqualityDeleteFilter(
	ctx context.Context,
	fs iceio.IO,
	deleteFiles []iceberg.DataFile,
	schema *iceberg.Schema,
	concurrency int,
) (*EqualityDeleteFilter, error) {
	if len(deleteFiles) == 0 {
		return nil, nil
	}

	// Filter to only equality deletes
	eqDeletes := make([]iceberg.DataFile, 0, len(deleteFiles))
	for _, d := range deleteFiles {
		if d.ContentType() == iceberg.EntryContentEqDeletes {
			eqDeletes = append(eqDeletes, d)
		}
	}

	if len(eqDeletes) == 0 {
		return nil, nil
	}

	mem := compute.GetAllocator(ctx)

	type result struct {
		rec      arrow.Record
		fieldIDs []int
	}

	resultChan := make(chan result, len(eqDeletes))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, df := range eqDeletes {
		g.Go(func() error {
			rec, fids, err := readEqualityDeleteFile(gctx, fs, df)
			if err != nil {
				return fmt.Errorf("read equality delete file %s: %w", df.FilePath(), err)
			}
			resultChan <- result{rec: rec, fieldIDs: fids}
			return nil
		})
	}

	// Close channel when all goroutines complete
	go func() {
		g.Wait()
		close(resultChan)
	}()

	var (
		records  []arrow.Record
		fieldIDs []int
		mu       sync.Mutex
	)

	for r := range resultChan {
		mu.Lock()
		records = append(records, r.rec)
		if fieldIDs == nil {
			fieldIDs = r.fieldIDs
		}
		mu.Unlock()
	}

	if err := g.Wait(); err != nil {
		for _, r := range records {
			r.Release()
		}
		return nil, err
	}

	return &EqualityDeleteFilter{
		deleteRecords: records,
		fieldIDs:      fieldIDs,
		schema:        schema,
		mem:           mem,
	}, nil
}

// readEqualityDeleteFile reads an equality delete file and returns its records.
func readEqualityDeleteFile(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (_ arrow.Record, fieldIDs []int, err error) {
	src, err := internal.GetFile(ctx, fs, dataFile, false)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tbl.Release()

	rec, err := tableToRecord(ctx, tbl)
	if err != nil {
		return nil, nil, fmt.Errorf("convert table to record: %w", err)
	}

	return rec, dataFile.EqualityFieldIDs(), nil
}

func tableToRecord(ctx context.Context, tbl arrow.Table) (arrow.Record, error) {
	mem := compute.GetAllocator(ctx)

	cols := make([]arrow.Array, tbl.NumCols())
	for i := int64(0); i < tbl.NumCols(); i++ {
		col := tbl.Column(int(i))
		if col.Len() == 0 {
			cols[i] = array.MakeFromData(array.NewData(col.DataType(), 0, nil, nil, 0, 0))
			continue
		}

		chunks := col.Data().Chunks()
		if len(chunks) == 1 {
			chunks[0].Retain()
			cols[i] = chunks[0]
		} else {
			result, err := array.Concatenate(chunks, mem)
			if err != nil {
				return nil, fmt.Errorf("concatenate chunks: %w", err)
			}
			cols[i] = result
		}
	}

	return array.NewRecord(tbl.Schema(), cols, tbl.NumRows()), nil
}

// Filter applies the equality delete filter to a record batch.
// Returns a new record with deleted rows removed.
func (f *EqualityDeleteFilter) Filter(ctx context.Context, rec arrow.Record, fileSchema *iceberg.Schema) (arrow.Record, error) {
	if f == nil || len(f.deleteRecords) == 0 {
		rec.Retain()
		return rec, nil
	}

	// Build column index mapping from data file to delete file
	colMapping, err := f.buildColumnMapping(rec.Schema(), fileSchema)
	if err != nil {
		return nil, err
	}

	// Create a mask of rows to keep (true = keep, false = delete)
	mask := f.buildDeleteMask(ctx, rec, colMapping)
	defer mask.Release()

	// Apply mask to filter the record
	result, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(rec), compute.NewDatumWithoutOwning(mask), *compute.DefaultFilterOptions())
	if err != nil {
		return nil, err
	}

	return result.(*compute.RecordDatum).Value, nil
}

// buildColumnMapping creates a mapping from data record column indices to delete record column indices.
func (f *EqualityDeleteFilter) buildColumnMapping(dataSchema *arrow.Schema, fileSchema *iceberg.Schema) (map[int]int, error) {
	mapping := make(map[int]int)

	if len(f.deleteRecords) == 0 {
		return mapping, nil
	}

	deleteSchema := f.deleteRecords[0].Schema()

	for i, fieldID := range f.fieldIDs {
		// Find field in file schema
		field, ok := fileSchema.FindFieldByID(fieldID)
		if !ok {
			return nil, fmt.Errorf("field ID %d not found in file schema", fieldID)
		}

		// Find column index in data schema
		dataIdx := -1
		for j := 0; j < dataSchema.NumFields(); j++ {
			if dataSchema.Field(j).Name == field.Name {
				dataIdx = j
				break
			}
		}
		if dataIdx < 0 {
			continue // Column not in projected schema
		}

		// Find column index in delete schema (should be positional)
		if i < deleteSchema.NumFields() {
			mapping[dataIdx] = i
		}
	}

	return mapping, nil
}

// buildDeleteMask creates a boolean mask where true = keep row, false = delete row.
func (f *EqualityDeleteFilter) buildDeleteMask(ctx context.Context, rec arrow.Record, colMapping map[int]int) arrow.Array {
	// Start with all true (keep all rows)
	bldr := array.NewBooleanBuilder(f.mem)
	defer bldr.Release()

	numRows := int(rec.NumRows())
	for i := 0; i < numRows; i++ {
		bldr.Append(true)
	}
	mask := bldr.NewArray()

	// For each delete record, mark matching rows as false
	for _, deleteRec := range f.deleteRecords {
		mask = f.applyDeleteRecord(ctx, rec, deleteRec, colMapping, mask)
	}

	return mask
}

// applyDeleteRecord marks rows in mask as false if they match any row in deleteRec.
func (f *EqualityDeleteFilter) applyDeleteRecord(ctx context.Context, dataRec arrow.Record, deleteRec arrow.Record, colMapping map[int]int, mask arrow.Array) arrow.Array {
	numDataRows := int(dataRec.NumRows())
	numDeleteRows := int(deleteRec.NumRows())

	if numDeleteRows == 0 {
		return mask
	}

	maskArr := mask.(*array.Boolean)
	bldr := array.NewBooleanBuilder(f.mem)
	defer bldr.Release()

	// For each data row, check if it matches any delete row
	for i := 0; i < numDataRows; i++ {
		if !maskArr.Value(i) {
			// Already marked for deletion
			bldr.Append(false)
			continue
		}

		matched := false
		for j := 0; j < numDeleteRows && !matched; j++ {
			if f.rowsMatch(dataRec, i, deleteRec, j, colMapping) {
				matched = true
			}
		}

		bldr.Append(!matched)
	}

	mask.Release()
	return bldr.NewArray()
}

// rowsMatch checks if a data row matches a delete row on all equality columns.
func (f *EqualityDeleteFilter) rowsMatch(dataRec arrow.Record, dataRow int, deleteRec arrow.Record, deleteRow int, colMapping map[int]int) bool {
	for dataCol, deleteCol := range colMapping {
		dataArr := dataRec.Column(dataCol)
		deleteArr := deleteRec.Column(deleteCol)

		if !valuesEqual(dataArr, dataRow, deleteArr, deleteRow) {
			return false
		}
	}
	return true
}

// valuesEqual compares values at specific indices in two arrays.
func valuesEqual(arr1 arrow.Array, idx1 int, arr2 arrow.Array, idx2 int) bool {
	if arr1.IsNull(idx1) && arr2.IsNull(idx2) {
		return true
	}
	if arr1.IsNull(idx1) || arr2.IsNull(idx2) {
		return false
	}

	// Compare based on array type
	switch a1 := arr1.(type) {
	case *array.Int32:
		a2, ok := arr2.(*array.Int32)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Int64:
		a2, ok := arr2.(*array.Int64)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Float32:
		a2, ok := arr2.(*array.Float32)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Float64:
		a2, ok := arr2.(*array.Float64)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.String:
		a2, ok := arr2.(*array.String)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.LargeString:
		a2, ok := arr2.(*array.LargeString)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Binary:
		a2, ok := arr2.(*array.Binary)
		if !ok {
			return false
		}
		return bytes.Equal(a1.Value(idx1), a2.Value(idx2))
	case *array.Boolean:
		a2, ok := arr2.(*array.Boolean)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Date32:
		a2, ok := arr2.(*array.Date32)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Date64:
		a2, ok := arr2.(*array.Date64)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Timestamp:
		a2, ok := arr2.(*array.Timestamp)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	case *array.Decimal128:
		a2, ok := arr2.(*array.Decimal128)
		return ok && a1.Value(idx1) == a2.Value(idx2)
	default:
		// For unsupported types, use string representation as fallback
		return arr1.ValueStr(idx1) == arr2.ValueStr(idx2)
	}
}

// Release releases resources held by the filter.
func (f *EqualityDeleteFilter) Release() {
	if f == nil {
		return
	}
	for _, r := range f.deleteRecords {
		r.Release()
	}
	f.deleteRecords = nil
}

// IsEmpty returns true if the filter has no delete records.
func (f *EqualityDeleteFilter) IsEmpty() bool {
	return f == nil || len(f.deleteRecords) == 0
}
