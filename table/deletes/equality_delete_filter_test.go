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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValuesEqual(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("int64 equal", func(t *testing.T) {
		bldr := array.NewInt64Builder(mem)
		bldr.AppendValues([]int64{1, 2, 3}, nil)
		arr1 := bldr.NewArray()
		defer arr1.Release()

		bldr.AppendValues([]int64{2, 2, 4}, nil)
		arr2 := bldr.NewArray()
		defer arr2.Release()

		assert.False(t, valuesEqual(arr1, 0, arr2, 0)) // 1 != 2
		assert.True(t, valuesEqual(arr1, 1, arr2, 1))  // 2 == 2
		assert.False(t, valuesEqual(arr1, 2, arr2, 2)) // 3 != 4
	})

	t.Run("string equal", func(t *testing.T) {
		bldr := array.NewStringBuilder(mem)
		bldr.AppendValues([]string{"a", "b", "c"}, nil)
		arr1 := bldr.NewArray()
		defer arr1.Release()

		bldr.AppendValues([]string{"a", "x", "c"}, nil)
		arr2 := bldr.NewArray()
		defer arr2.Release()

		assert.True(t, valuesEqual(arr1, 0, arr2, 0))  // "a" == "a"
		assert.False(t, valuesEqual(arr1, 1, arr2, 1)) // "b" != "x"
		assert.True(t, valuesEqual(arr1, 2, arr2, 2))  // "c" == "c"
	})

	t.Run("null handling", func(t *testing.T) {
		bldr := array.NewInt64Builder(mem)
		bldr.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
		arr1 := bldr.NewArray()
		defer arr1.Release()

		bldr.AppendValues([]int64{1, 0, 0}, []bool{true, false, false})
		arr2 := bldr.NewArray()
		defer arr2.Release()

		assert.True(t, valuesEqual(arr1, 0, arr2, 0))  // 1 == 1
		assert.True(t, valuesEqual(arr1, 1, arr2, 1))  // null == null
		assert.False(t, valuesEqual(arr1, 2, arr2, 2)) // 3 != null
	})
}

func TestEqualityDeleteFilterEmpty(t *testing.T) {
	filter, err := NewEqualityDeleteFilter(context.Background(), nil, nil, nil, 1)
	require.NoError(t, err)
	assert.True(t, filter.IsEmpty())
}

func TestEqualityDeleteFilterRowsMatch(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create data record
	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	idBldr.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	nameBldr := array.NewStringBuilder(mem)
	nameBldr.AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	nameArr := nameBldr.NewArray()
	defer nameArr.Release()

	valueBldr := array.NewFloat64Builder(mem)
	valueBldr.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	valueArr := valueBldr.NewArray()
	defer valueArr.Release()

	dataRec := array.NewRecord(dataSchema, []arrow.Array{idArr, nameArr, valueArr}, 5)
	defer dataRec.Release()

	// Create delete record (delete where id=2 or id=4)
	deleteSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	delIdBldr := array.NewInt64Builder(mem)
	delIdBldr.AppendValues([]int64{2, 4}, nil)
	delIdArr := delIdBldr.NewArray()
	defer delIdArr.Release()

	deleteRec := array.NewRecord(deleteSchema, []arrow.Array{delIdArr}, 2)
	defer deleteRec.Release()

	// Create filter
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	filter := &EqualityDeleteFilter{
		deleteRecords: []arrow.Record{deleteRec},
		fieldIDs:      []int{1}, // id field
		schema:        icebergSchema,
		mem:           mem,
	}

	// Test column mapping
	colMapping, err := filter.buildColumnMapping(dataSchema, icebergSchema)
	require.NoError(t, err)
	assert.Equal(t, map[int]int{0: 0}, colMapping) // data col 0 (id) maps to delete col 0

	// Test row matching
	assert.False(t, filter.rowsMatch(dataRec, 0, deleteRec, 0, colMapping)) // id=1 != id=2
	assert.True(t, filter.rowsMatch(dataRec, 1, deleteRec, 0, colMapping))  // id=2 == id=2
	assert.False(t, filter.rowsMatch(dataRec, 2, deleteRec, 0, colMapping)) // id=3 != id=2
	assert.True(t, filter.rowsMatch(dataRec, 3, deleteRec, 1, colMapping))  // id=4 == id=4
}

func TestEqualityDeleteFilterApply(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	// Create data record with 5 rows
	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	idBldr.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	nameBldr := array.NewStringBuilder(mem)
	nameBldr.AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	nameArr := nameBldr.NewArray()
	defer nameArr.Release()

	dataRec := array.NewRecord(dataSchema, []arrow.Array{idArr, nameArr}, 5)
	defer dataRec.Release()

	// Create delete record (delete where id=2 or id=4)
	deleteSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	delIdBldr := array.NewInt64Builder(mem)
	delIdBldr.AppendValues([]int64{2, 4}, nil)
	delIdArr := delIdBldr.NewArray()
	defer delIdArr.Release()

	deleteRec := array.NewRecord(deleteSchema, []arrow.Array{delIdArr}, 2)
	defer deleteRec.Release()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	filter := &EqualityDeleteFilter{
		deleteRecords: []arrow.Record{deleteRec},
		fieldIDs:      []int{1},
		schema:        icebergSchema,
		mem:           mem,
	}

	// Apply filter
	result, err := filter.Filter(ctx, dataRec, icebergSchema)
	require.NoError(t, err)
	defer result.Release()

	// Should have 3 rows remaining (1, 3, 5)
	assert.Equal(t, int64(3), result.NumRows())

	// Verify remaining ids
	resultIds := result.Column(0).(*array.Int64)
	assert.Equal(t, int64(1), resultIds.Value(0))
	assert.Equal(t, int64(3), resultIds.Value(1))
	assert.Equal(t, int64(5), resultIds.Value(2))
}

func TestEqualityDeleteFilterNilFilter(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := context.Background()

	// Create a simple record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewInt64Builder(mem)
	bldr.AppendValues([]int64{1, 2, 3}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	rec := array.NewRecord(schema, []arrow.Array{arr}, 3)
	defer rec.Release()

	// Nil filter should return original record
	var filter *EqualityDeleteFilter
	result, err := filter.Filter(ctx, rec, nil)
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, int64(3), result.NumRows())
}
