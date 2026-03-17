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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
)

type mockDeleteFile struct {
	path        string
	specID      int32
	content     iceberg.ManifestEntryContent
	lowerBounds map[int][]byte
	upperBounds map[int][]byte
}

func (m *mockDeleteFile) ContentType() iceberg.ManifestEntryContent { return m.content }
func (m *mockDeleteFile) FilePath() string                          { return m.path }
func (m *mockDeleteFile) FileFormat() iceberg.FileFormat            { return iceberg.ParquetFile }
func (m *mockDeleteFile) Partition() map[int]any                    { return nil }
func (m *mockDeleteFile) Count() int64                              { return 0 }
func (m *mockDeleteFile) FileSizeBytes() int64                      { return 0 }
func (m *mockDeleteFile) ColumnSizes() map[int]int64                { return nil }
func (m *mockDeleteFile) ValueCounts() map[int]int64                { return nil }
func (m *mockDeleteFile) NullValueCounts() map[int]int64            { return nil }
func (m *mockDeleteFile) NaNValueCounts() map[int]int64             { return nil }
func (m *mockDeleteFile) DistinctValueCounts() map[int]int64        { return nil }
func (m *mockDeleteFile) LowerBoundValues() map[int][]byte          { return m.lowerBounds }
func (m *mockDeleteFile) UpperBoundValues() map[int][]byte          { return m.upperBounds }
func (m *mockDeleteFile) KeyMetadata() []byte                       { return nil }
func (m *mockDeleteFile) SplitOffsets() []int64                     { return nil }
func (m *mockDeleteFile) EqualityFieldIDs() []int                   { return nil }
func (m *mockDeleteFile) SortOrderID() *int                         { return nil }
func (m *mockDeleteFile) SpecID() int32                             { return m.specID }
func (m *mockDeleteFile) FirstRowID() *int64                        { return nil }
func (m *mockDeleteFile) ReferencedDataFile() *string               { return nil }
func (m *mockDeleteFile) ContentOffset() *int64                     { return nil }
func (m *mockDeleteFile) ContentSizeInBytes() *int64                { return nil }

func newMockPositionDeleteFile(path string, specID int32) *mockDeleteFile {
	return &mockDeleteFile{
		path:    path,
		specID:  specID,
		content: iceberg.EntryContentPosDeletes,
	}
}

func newMockPositionDeleteFileWithPath(path string, specID int32, targetPath string) *mockDeleteFile {
	pathBytes := []byte(targetPath)
	return &mockDeleteFile{
		path:        path,
		specID:      specID,
		content:     iceberg.EntryContentPosDeletes,
		lowerBounds: map[int][]byte{2147483546: pathBytes},
		upperBounds: map[int][]byte{2147483546: pathBytes},
	}
}

func newMockDataFile(path string, specID int32) *mockDeleteFile {
	return &mockDeleteFile{
		path:    path,
		specID:  specID,
		content: iceberg.EntryContentData,
	}
}

func TestDeleteFileIndexEmpty(t *testing.T) {
	index := NewDeleteFileIndex()
	assert.True(t, index.IsEmpty())

	dataFile := newMockDataFile("/data/file1.parquet", 0)
	deletes := index.ForDataFile(dataFile, 1, "{}")
	assert.Empty(t, deletes)
}

func TestDeleteFileIndexSequenceNumberFiltering(t *testing.T) {
	index := NewDeleteFileIndex()

	// Add delete files with different sequence numbers
	delete1 := newMockPositionDeleteFile("/delete/d1.parquet", 0)
	delete2 := newMockPositionDeleteFile("/delete/d2.parquet", 0)
	delete3 := newMockPositionDeleteFile("/delete/d3.parquet", 0)

	index.AddDeleteFile(delete1, 5, "{}")
	index.AddDeleteFile(delete2, 10, "{}")
	index.AddDeleteFile(delete3, 15, "{}")

	assert.False(t, index.IsEmpty())

	dataFile := newMockDataFile("/data/file1.parquet", 0)

	// Data file with seq 3 should see all delete files (seq >= 3)
	deletes := index.ForDataFile(dataFile, 3, "{}")
	assert.Len(t, deletes, 3)

	// Data file with seq 7 should see delete files with seq >= 7
	deletes = index.ForDataFile(dataFile, 7, "{}")
	assert.Len(t, deletes, 2)

	// Data file with seq 12 should see only delete file with seq 15
	deletes = index.ForDataFile(dataFile, 12, "{}")
	assert.Len(t, deletes, 1)
	assert.Equal(t, "/delete/d3.parquet", deletes[0].FilePath())

	// Data file with seq 20 should see no delete files
	deletes = index.ForDataFile(dataFile, 20, "{}")
	assert.Empty(t, deletes)
}

func TestDeleteFileIndexPathSpecificDeletes(t *testing.T) {
	index := NewDeleteFileIndex()

	// Add a path-specific delete file
	targetPath := "/data/file1.parquet"
	deleteFile := newMockPositionDeleteFileWithPath("/delete/d1.parquet", 0, targetPath)
	index.AddDeleteFile(deleteFile, 5, "{}")

	// Add a partition-based delete file
	partitionDelete := newMockPositionDeleteFile("/delete/d2.parquet", 0)
	index.AddDeleteFile(partitionDelete, 5, "{}")

	// Data file matching the path should see both deletes
	dataFile1 := newMockDataFile(targetPath, 0)
	deletes := index.ForDataFile(dataFile1, 3, "{}")
	assert.Len(t, deletes, 2)

	// Different data file should only see partition-based delete
	dataFile2 := newMockDataFile("/data/file2.parquet", 0)
	deletes = index.ForDataFile(dataFile2, 3, "{}")
	assert.Len(t, deletes, 1)
	assert.Equal(t, "/delete/d2.parquet", deletes[0].FilePath())
}

func TestDeleteFileIndexPartitionIsolation(t *testing.T) {
	index := NewDeleteFileIndex()

	// Add delete files for different partitions
	delete1 := newMockPositionDeleteFile("/delete/d1.parquet", 0)
	delete2 := newMockPositionDeleteFile("/delete/d2.parquet", 0)

	index.AddDeleteFile(delete1, 5, `{"date":"2024-01-01"}`)
	index.AddDeleteFile(delete2, 5, `{"date":"2024-01-02"}`)

	// Data file in partition 1 should only see delete1
	dataFile1 := newMockDataFile("/data/file1.parquet", 0)
	deletes := index.ForDataFile(dataFile1, 3, `{"date":"2024-01-01"}`)
	assert.Len(t, deletes, 1)
	assert.Equal(t, "/delete/d1.parquet", deletes[0].FilePath())

	// Data file in partition 2 should only see delete2
	dataFile2 := newMockDataFile("/data/file2.parquet", 0)
	deletes = index.ForDataFile(dataFile2, 3, `{"date":"2024-01-02"}`)
	assert.Len(t, deletes, 1)
	assert.Equal(t, "/delete/d2.parquet", deletes[0].FilePath())

	// Data file in a different partition should see no deletes
	dataFile3 := newMockDataFile("/data/file3.parquet", 0)
	deletes = index.ForDataFile(dataFile3, 3, `{"date":"2024-01-03"}`)
	assert.Empty(t, deletes)
}

func TestPositionDeletesSorting(t *testing.T) {
	pd := &positionDeletes{}

	// Add in non-sorted order
	pd.add(newMockPositionDeleteFile("/d3.parquet", 0), 15)
	pd.add(newMockPositionDeleteFile("/d1.parquet", 0), 5)
	pd.add(newMockPositionDeleteFile("/d2.parquet", 0), 10)

	// Filter should return sorted and filtered results
	result := pd.filterBySeq(8)
	assert.Len(t, result, 2)
	assert.Equal(t, "/d2.parquet", result[0].FilePath())
	assert.Equal(t, "/d3.parquet", result[1].FilePath())
}
