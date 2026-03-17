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
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
)

// positionDeletes collects position delete files and indexes them by sequence number.
type positionDeletes struct {
	buffer []deleteWithSeq
	sorted bool
}

type deleteWithSeq struct {
	file   iceberg.DataFile
	seqNum int64
}

func (p *positionDeletes) add(deleteFile iceberg.DataFile, seqNum int64) {
	p.buffer = append(p.buffer, deleteWithSeq{file: deleteFile, seqNum: seqNum})
	p.sorted = false
}

func (p *positionDeletes) ensureSorted() {
	if p.sorted {
		return
	}
	sort.Slice(p.buffer, func(i, j int) bool {
		return p.buffer[i].seqNum < p.buffer[j].seqNum
	})
	p.sorted = true
}

// filterBySeq returns delete files with sequence number >= seq.
// Only delete files written at or after the data file's sequence
// number can affect that data file.
func (p *positionDeletes) filterBySeq(seq int64) []iceberg.DataFile {
	p.ensureSorted()
	if len(p.buffer) == 0 {
		return nil
	}

	// Binary search for first element >= seq
	idx := sort.Search(len(p.buffer), func(i int) bool {
		return p.buffer[i].seqNum >= seq
	})

	result := make([]iceberg.DataFile, 0, len(p.buffer)-idx)
	for i := idx; i < len(p.buffer); i++ {
		result = append(result, p.buffer[i].file)
	}
	return result
}

// partitionKey creates a key for partition-based indexing.
type partitionKey struct {
	specID    int
	partition string // JSON-serialized partition values
}

// DeleteFileIndex indexes delete files by partition and by exact data file path.
// This allows efficient lookup of which delete files apply to a given data file.
type DeleteFileIndex struct {
	byPartition map[partitionKey]*positionDeletes
	byPath      map[string]*positionDeletes
}

// NewDeleteFileIndex creates a new empty delete file index.
func NewDeleteFileIndex() *DeleteFileIndex {
	return &DeleteFileIndex{
		byPartition: make(map[partitionKey]*positionDeletes),
		byPath:      make(map[string]*positionDeletes),
	}
}

// AddDeleteFile adds a delete file to the index.
// The sequenceNumber should come from the ManifestEntry.
// partitionValues is the JSON-serialized partition for partition-based lookups.
func (d *DeleteFileIndex) AddDeleteFile(deleteFile iceberg.DataFile, sequenceNumber int64, partitionValues string) {
	// Check if this delete file references a specific data file path
	// by checking if lower_bounds == upper_bounds for the file_path field
	targetPath := d.referencedDataFilePath(deleteFile)

	if targetPath != "" {
		// Add to path-specific index
		deletes := d.byPath[targetPath]
		if deletes == nil {
			deletes = &positionDeletes{}
			d.byPath[targetPath] = deletes
		}
		deletes.add(deleteFile, sequenceNumber)
	} else {
		// Add to partition-based index
		key := partitionKey{
			specID:    int(deleteFile.SpecID()),
			partition: partitionValues,
		}
		deletes := d.byPartition[key]
		if deletes == nil {
			deletes = &positionDeletes{}
			d.byPartition[key] = deletes
		}
		deletes.add(deleteFile, sequenceNumber)
	}
}

// referencedDataFilePath returns the data file path if the delete file
// references a specific file (lower_bounds == upper_bounds for file_path field).
// Returns empty string if the delete file applies to multiple data files.
func (d *DeleteFileIndex) referencedDataFilePath(deleteFile iceberg.DataFile) string {
	// Field ID 2147483546 is the reserved field ID for file_path in delete files
	const filePathFieldID = 2147483546

	lowerBounds := deleteFile.LowerBoundValues()
	upperBounds := deleteFile.UpperBoundValues()

	if lowerBounds == nil || upperBounds == nil {
		return ""
	}

	lower, hasLower := lowerBounds[filePathFieldID]
	upper, hasUpper := upperBounds[filePathFieldID]

	if !hasLower || !hasUpper {
		return ""
	}

	if !slices.Equal(lower, upper) {
		return ""
	}

	return string(lower)
}

// ForDataFile returns all delete files that apply to the given data file.
// The sequenceNumber should be the data file's sequence number from its manifest entry.
func (d *DeleteFileIndex) ForDataFile(dataFile iceberg.DataFile, sequenceNumber int64, partitionValues string) []iceberg.DataFile {
	var result []iceberg.DataFile

	// Check partition-based deletes
	key := partitionKey{
		specID:    int(dataFile.SpecID()),
		partition: partitionValues,
	}
	if partitionDeletes := d.byPartition[key]; partitionDeletes != nil {
		result = append(result, partitionDeletes.filterBySeq(sequenceNumber)...)
	}

	// Check path-specific deletes
	if pathDeletes := d.byPath[dataFile.FilePath()]; pathDeletes != nil {
		result = append(result, pathDeletes.filterBySeq(sequenceNumber)...)
	}

	return result
}

// IsEmpty returns true if the index contains no delete files.
func (d *DeleteFileIndex) IsEmpty() bool {
	return len(d.byPartition) == 0 && len(d.byPath) == 0
}
