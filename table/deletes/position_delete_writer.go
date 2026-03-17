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
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/hamba/avro/v2"
)

// PositionDeleteWriter writes position delete files for Iceberg tables.
// Position deletes identify rows to delete by file path and row position.
type PositionDeleteWriter struct {
	fs   iceio.WriteFileIO
	spec iceberg.PartitionSpec
	mem  memory.Allocator
}

// NewPositionDeleteWriter creates a new writer for position delete files.
func NewPositionDeleteWriter(
	ctx context.Context,
	fs iceio.WriteFileIO,
	spec iceberg.PartitionSpec,
) *PositionDeleteWriter {
	return &PositionDeleteWriter{
		fs:   fs,
		spec: spec,
		mem:  compute.GetAllocator(ctx),
	}
}

// WriteDeleteFile writes a position delete file.
// The records must have the positional delete schema: (file_path string, pos int64).
// Returns the DataFile metadata for the written delete file.
func (w *PositionDeleteWriter) WriteDeleteFile(
	ctx context.Context,
	path string,
	records []arrow.Record,
	partitionValues map[int]any,
) (_ iceberg.DataFile, err error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("%w: no records to write", iceberg.ErrInvalidArgument)
	}

	// Validate schema matches positional delete schema
	if err := w.validateSchema(records[0].Schema()); err != nil {
		return nil, err
	}

	out, err := w.fs.Create(path)
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(out, &err)

	counter := &internal.CountingWriter{W: out}

	writerProps := w.getWriterProperties()
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(w.mem), pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(records[0].Schema(), counter, writerProps, arrProps)
	if err != nil {
		return nil, err
	}

	var recordCount int64
	for _, rec := range records {
		if err := writer.WriteBuffered(rec); err != nil {
			return nil, err
		}
		recordCount += rec.NumRows()
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return w.buildDataFile(path, counter.Count, recordCount, partitionValues)
}

func (w *PositionDeleteWriter) validateSchema(schema *arrow.Schema) error {
	if schema.NumFields() < 2 {
		return fmt.Errorf("%w: position delete schema must have at least 2 fields (file_path, pos)", iceberg.ErrInvalidSchema)
	}

	// Check first field is string (file_path)
	if schema.Field(0).Type.ID() != arrow.STRING {
		return fmt.Errorf("%w: first field must be string (file_path), got %s", iceberg.ErrInvalidSchema, schema.Field(0).Type)
	}

	// Check second field is int64 (pos)
	if schema.Field(1).Type.ID() != arrow.INT64 {
		return fmt.Errorf("%w: second field must be int64 (pos), got %s", iceberg.ErrInvalidSchema, schema.Field(1).Type)
	}

	return nil
}

func (w *PositionDeleteWriter) getWriterProperties() *parquet.WriterProperties {
	return parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true), // Enable dictionary for file_path column
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDataPageVersion(parquet.DataPageV2),
	)
}

func (w *PositionDeleteWriter) buildDataFile(
	path string,
	fileSize int64,
	recordCount int64,
	partitionValues map[int]any,
) (iceberg.DataFile, error) {
	fieldIDToPartitionData := make(map[int]any)
	fieldIDToLogicalType := make(map[int]avro.LogicalType)
	fieldIDToFixedSize := make(map[int]int)

	if !w.spec.Equals(*iceberg.UnpartitionedSpec) {
		for _, field := range w.spec.Fields() {
			if val, ok := partitionValues[field.FieldID]; ok {
				fieldIDToPartitionData[field.FieldID] = val
			} else {
				fieldIDToPartitionData[field.FieldID] = nil
			}
		}
	}

	bldr, err := iceberg.NewDataFileBuilder(
		w.spec,
		iceberg.EntryContentPosDeletes,
		path,
		iceberg.ParquetFile,
		fieldIDToPartitionData,
		fieldIDToLogicalType,
		fieldIDToFixedSize,
		recordCount,
		fileSize,
	)
	if err != nil {
		return nil, err
	}

	return bldr.Build(), nil
}

// PositionDelete represents a single position delete entry.
type PositionDelete struct {
	FilePath string
	Pos      int64
}

// PositionDeleteBuilder helps build Arrow records for position deletes.
type PositionDeleteBuilder struct {
	mem       memory.Allocator
	paths     *array.StringBuilder
	positions *array.Int64Builder
}

// NewPositionDeleteBuilder creates a new builder for position delete records.
func NewPositionDeleteBuilder(mem memory.Allocator) *PositionDeleteBuilder {
	return &PositionDeleteBuilder{
		mem:       mem,
		paths:     array.NewStringBuilder(mem),
		positions: array.NewInt64Builder(mem),
	}
}

// Add adds a position delete entry.
func (b *PositionDeleteBuilder) Add(filePath string, pos int64) {
	b.paths.Append(filePath)
	b.positions.Append(pos)
}

// AddBatch adds multiple position delete entries.
func (b *PositionDeleteBuilder) AddBatch(deletes []PositionDelete) {
	for _, d := range deletes {
		b.Add(d.FilePath, d.Pos)
	}
}

// Build creates an Arrow record from the accumulated deletes.
// The caller is responsible for releasing the record.
func (b *PositionDeleteBuilder) Build() arrow.Record {
	pathArr := b.paths.NewArray()
	posArr := b.positions.NewArray()

	schema := PositionDeleteArrowSchema()
	return array.NewRecord(schema, []arrow.Array{pathArr, posArr}, int64(pathArr.Len()))
}

// Release releases the builder's resources.
func (b *PositionDeleteBuilder) Release() {
	b.paths.Release()
	b.positions.Release()
}

// PositionDeleteArrowSchema returns the Arrow schema for position delete files.
func PositionDeleteArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}
