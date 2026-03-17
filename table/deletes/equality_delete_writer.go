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

// EqualityDeleteWriter writes equality delete files for Iceberg tables.
// Equality deletes identify rows to delete by matching values in specified columns.
type EqualityDeleteWriter struct {
	fs               iceio.WriteFileIO
	schema           *iceberg.Schema
	spec             iceberg.PartitionSpec
	equalityFieldIDs []int
	props            iceberg.Properties
	mem              memory.Allocator
}

// NewEqualityDeleteWriter creates a new writer for equality delete files.
// The equalityFieldIDs specify which columns are used to identify rows to delete.
func NewEqualityDeleteWriter(
	ctx context.Context,
	fs iceio.WriteFileIO,
	schema *iceberg.Schema,
	spec iceberg.PartitionSpec,
	equalityFieldIDs []int,
	props iceberg.Properties,
) (*EqualityDeleteWriter, error) {
	if len(equalityFieldIDs) == 0 {
		return nil, fmt.Errorf("%w: equality field IDs cannot be empty", iceberg.ErrInvalidArgument)
	}

	// Validate that all equality field IDs exist in the schema
	for _, fieldID := range equalityFieldIDs {
		if _, ok := schema.FindFieldByID(fieldID); !ok {
			return nil, fmt.Errorf("%w: equality field ID %d not found in schema", iceberg.ErrInvalidSchema, fieldID)
		}
	}

	return &EqualityDeleteWriter{
		fs:               fs,
		schema:           schema,
		spec:             spec,
		equalityFieldIDs: equalityFieldIDs,
		props:            props,
		mem:              compute.GetAllocator(ctx),
	}, nil
}

// WriteDeleteFile writes a delete file containing the records to delete.
// The records should contain only the equality field columns.
// Returns the DataFile metadata for the written delete file.
func (w *EqualityDeleteWriter) WriteDeleteFile(
	ctx context.Context,
	path string,
	records []arrow.Record,
	partitionValues map[int]any,
) (_ iceberg.DataFile, err error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("%w: no records to write", iceberg.ErrInvalidArgument)
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

// WriteDeleteRecords writes a delete file from Arrow records directly.
// This is a convenience method that accepts an array.RecordReader.
func (w *EqualityDeleteWriter) WriteDeleteRecords(
	ctx context.Context,
	path string,
	reader array.RecordReader,
	partitionValues map[int]any,
) (iceberg.DataFile, error) {
	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}

	if reader.Err() != nil {
		for _, r := range records {
			r.Release()
		}
		return nil, reader.Err()
	}

	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	return w.WriteDeleteFile(ctx, path, records, partitionValues)
}

func (w *EqualityDeleteWriter) getWriterProperties() *parquet.WriterProperties {
	return parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDataPageVersion(parquet.DataPageV2),
	)
}

func (w *EqualityDeleteWriter) buildDataFile(
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

			if sourceField, ok := w.schema.FindFieldByID(field.SourceID); ok {
				resultType := field.Transform.ResultType(sourceField.Type)

				switch rt := resultType.(type) {
				case iceberg.DateType:
					fieldIDToLogicalType[field.FieldID] = avro.Date
				case iceberg.TimeType:
					fieldIDToLogicalType[field.FieldID] = avro.TimeMicros
				case iceberg.TimestampType:
					fieldIDToLogicalType[field.FieldID] = avro.TimestampMicros
				case iceberg.TimestampTzType:
					fieldIDToLogicalType[field.FieldID] = avro.TimestampMicros
				case iceberg.DecimalType:
					fieldIDToLogicalType[field.FieldID] = avro.Decimal
					fieldIDToFixedSize[field.FieldID] = rt.Scale()
				case iceberg.UUIDType:
					fieldIDToLogicalType[field.FieldID] = avro.UUID
				}
			}
		}
	}

	bldr, err := iceberg.NewDataFileBuilder(
		w.spec,
		iceberg.EntryContentEqDeletes,
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

	bldr.EqualityFieldIDs(w.equalityFieldIDs)

	return bldr.Build(), nil
}

// DeleteSchema returns a schema containing only the equality delete fields.
// This is the schema that should be used for the delete records.
func (w *EqualityDeleteWriter) DeleteSchema() (*iceberg.Schema, error) {
	var fields []iceberg.NestedField

	for _, fieldID := range w.equalityFieldIDs {
		field, ok := w.schema.FindFieldByID(fieldID)
		if !ok {
			return nil, fmt.Errorf("%w: field ID %d not found", iceberg.ErrInvalidSchema, fieldID)
		}
		fields = append(fields, field)
	}

	return iceberg.NewSchema(0, fields...), nil
}

// EqualityFieldIDs returns the field IDs used for equality matching.
func (w *EqualityDeleteWriter) EqualityFieldIDs() []int {
	return w.equalityFieldIDs
}
