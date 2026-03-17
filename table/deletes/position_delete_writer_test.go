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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPositionDeleteWriter(t *testing.T) {
	ctx := context.Background()
	writer := NewPositionDeleteWriter(ctx, nil, *iceberg.UnpartitionedSpec)
	assert.NotNil(t, writer)
}

func TestPositionDeleteArrowSchema(t *testing.T) {
	schema := PositionDeleteArrowSchema()

	assert.Equal(t, 2, schema.NumFields())
	assert.Equal(t, "file_path", schema.Field(0).Name)
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(0).Type)
	assert.Equal(t, "pos", schema.Field(1).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, schema.Field(1).Type)
}

func TestPositionDeleteBuilder(t *testing.T) {
	mem := memory.NewGoAllocator()
	builder := NewPositionDeleteBuilder(mem)
	defer builder.Release()

	builder.Add("/data/file1.parquet", 0)
	builder.Add("/data/file1.parquet", 5)
	builder.Add("/data/file2.parquet", 10)

	rec := builder.Build()
	defer rec.Release()

	assert.Equal(t, int64(3), rec.NumRows())
	assert.Equal(t, 2, int(rec.NumCols()))

	// Verify schema
	schema := rec.Schema()
	assert.Equal(t, "file_path", schema.Field(0).Name)
	assert.Equal(t, "pos", schema.Field(1).Name)
}

func TestPositionDeleteBuilderAddBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	builder := NewPositionDeleteBuilder(mem)
	defer builder.Release()

	deletes := []PositionDelete{
		{FilePath: "/data/file1.parquet", Pos: 0},
		{FilePath: "/data/file1.parquet", Pos: 5},
		{FilePath: "/data/file2.parquet", Pos: 10},
	}

	builder.AddBatch(deletes)

	rec := builder.Build()
	defer rec.Release()

	assert.Equal(t, int64(3), rec.NumRows())
}

func TestPositionDeleteWriterValidateSchema(t *testing.T) {
	ctx := context.Background()
	writer := NewPositionDeleteWriter(ctx, nil, *iceberg.UnpartitionedSpec)

	t.Run("valid schema", func(t *testing.T) {
		schema := PositionDeleteArrowSchema()
		err := writer.validateSchema(schema)
		require.NoError(t, err)
	})

	t.Run("too few fields", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "file_path", Type: arrow.BinaryTypes.String},
		}, nil)
		err := writer.validateSchema(schema)
		assert.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	})

	t.Run("wrong first field type", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "file_path", Type: arrow.PrimitiveTypes.Int64},
			{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		err := writer.validateSchema(schema)
		assert.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	})

	t.Run("wrong second field type", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "file_path", Type: arrow.BinaryTypes.String},
			{Name: "pos", Type: arrow.BinaryTypes.String},
		}, nil)
		err := writer.validateSchema(schema)
		assert.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	})
}
