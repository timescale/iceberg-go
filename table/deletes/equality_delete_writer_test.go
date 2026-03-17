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

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEqualityDeleteWriter(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	ctx := context.Background()

	t.Run("valid equality fields", func(t *testing.T) {
		writer, err := NewEqualityDeleteWriter(ctx, nil, schema, *iceberg.UnpartitionedSpec, []int{1}, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{1}, writer.EqualityFieldIDs())
	})

	t.Run("multiple equality fields", func(t *testing.T) {
		writer, err := NewEqualityDeleteWriter(ctx, nil, schema, *iceberg.UnpartitionedSpec, []int{1, 2}, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, writer.EqualityFieldIDs())
	})

	t.Run("empty equality fields", func(t *testing.T) {
		_, err := NewEqualityDeleteWriter(ctx, nil, schema, *iceberg.UnpartitionedSpec, []int{}, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("invalid field ID", func(t *testing.T) {
		_, err := NewEqualityDeleteWriter(ctx, nil, schema, *iceberg.UnpartitionedSpec, []int{999}, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	})
}

func TestEqualityDeleteWriterDeleteSchema(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	ctx := context.Background()

	t.Run("single field schema", func(t *testing.T) {
		writer, err := NewEqualityDeleteWriter(ctx, nil, schema, *iceberg.UnpartitionedSpec, []int{1}, nil)
		require.NoError(t, err)

		deleteSchema, err := writer.DeleteSchema()
		require.NoError(t, err)

		assert.Len(t, deleteSchema.Fields(), 1)
		assert.Equal(t, "id", deleteSchema.Fields()[0].Name)
		assert.Equal(t, 1, deleteSchema.Fields()[0].ID)
	})

	t.Run("multiple field schema", func(t *testing.T) {
		writer, err := NewEqualityDeleteWriter(ctx, nil, schema, *iceberg.UnpartitionedSpec, []int{1, 2}, nil)
		require.NoError(t, err)

		deleteSchema, err := writer.DeleteSchema()
		require.NoError(t, err)

		assert.Len(t, deleteSchema.Fields(), 2)
		assert.Equal(t, "id", deleteSchema.Fields()[0].Name)
		assert.Equal(t, "name", deleteSchema.Fields()[1].Name)
	})
}
