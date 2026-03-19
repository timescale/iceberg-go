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

package table

import (
	"fmt"
	"maps"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

// rowDeltaProducer handles the creation of snapshots that include both
// data files and delete files. This is used for CDC-style operations
// where updates are represented as deletes + inserts.
type rowDeltaProducer struct {
	*snapshotProducer

	addedDeleteFiles []iceberg.DataFile
}

func newRowDeltaProducer(txn *Transaction, fs iceio.WriteFileIO, snapshotProps iceberg.Properties) *rowDeltaProducer {
	commit := uuid.New()
	var parentSnapshot int64 = -1

	if snap := txn.meta.currentSnapshot(); snap != nil {
		parentSnapshot = snap.SnapshotID
	}

	base := &snapshotProducer{
		commitUuid:       commit,
		io:               fs,
		txn:              txn,
		op:               OpOverwrite, // RowDelta uses OVERWRITE operation
		snapshotID:       txn.meta.newSnapshotID(),
		parentSnapshotID: parentSnapshot,
		addedFiles:       []iceberg.DataFile{},
		deletedFiles:     make(map[string]iceberg.DataFile),
		snapshotProps:    snapshotProps,
		targetBranch:     MainBranch,
	}

	prod := &rowDeltaProducer{
		snapshotProducer: base,
		addedDeleteFiles: make([]iceberg.DataFile, 0),
	}

	// Set the producer implementation
	base.producerImpl = prod

	return prod
}

func (rd *rowDeltaProducer) appendDeleteFile(df iceberg.DataFile) *rowDeltaProducer {
	rd.addedDeleteFiles = append(rd.addedDeleteFiles, df)
	return rd
}

// processManifests implements producerImpl interface
func (rd *rowDeltaProducer) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	return manifests, nil
}

// existingManifests implements producerImpl interface - returns existing manifests from parent snapshot
func (rd *rowDeltaProducer) existingManifests() ([]iceberg.ManifestFile, error) {
	existing := make([]iceberg.ManifestFile, 0)

	if rd.parentSnapshotID <= 0 {
		return existing, nil
	}

	previous, err := rd.txn.meta.SnapshotByID(rd.parentSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("could not find parent snapshot %d", rd.parentSnapshotID)
	}

	manifests, err := previous.Manifests(rd.io)
	if err != nil {
		return nil, err
	}

	for _, m := range manifests {
		if m.HasAddedFiles() || m.HasExistingFiles() || m.SnapshotID() == rd.snapshotID {
			existing = append(existing, m)
		}
	}

	return existing, nil
}

// deletedEntries implements producerImpl interface - RowDelta doesn't mark existing files as deleted
func (rd *rowDeltaProducer) deletedEntries() ([]iceberg.ManifestEntry, error) {
	return nil, nil
}

// manifests overrides the base implementation to also write delete file manifests
func (rd *rowDeltaProducer) manifests() ([]iceberg.ManifestFile, error) {
	var results []iceberg.ManifestFile

	// Write data file manifest (if any)
	if len(rd.addedFiles) > 0 {
		dataManifest, err := rd.writeDataManifest()
		if err != nil {
			return nil, err
		}
		results = append(results, dataManifest)
	}

	// Write delete file manifest (if any)
	if len(rd.addedDeleteFiles) > 0 {
		deleteManifest, err := rd.writeDeleteManifest()
		if err != nil {
			return nil, err
		}
		results = append(results, deleteManifest)
	}

	// Get existing manifests from parent snapshot
	existing, err := rd.existingManifests()
	if err != nil {
		return nil, err
	}
	results = append(results, existing...)

	return rd.processManifests(results)
}

func (rd *rowDeltaProducer) writeDataManifest() (_ iceberg.ManifestFile, err error) {
	out, path, err := rd.newManifestOutput()
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(out, &err)

	counter := &internal.CountingWriter{W: out}
	currentSpec, err := rd.txn.meta.CurrentSpec()
	if err != nil {
		return nil, fmt.Errorf("could not get current partition spec: %w", err)
	}
	if currentSpec == nil {
		return nil, fmt.Errorf("current partition spec is nil")
	}

	wr, err := iceberg.NewManifestWriter(rd.txn.meta.formatVersion, counter,
		*currentSpec, rd.txn.meta.CurrentSchema(), rd.snapshotID)
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(wr, &err)

	for _, df := range rd.addedFiles {
		if err := wr.Add(iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &rd.snapshotID, nil, nil, df)); err != nil {
			return nil, err
		}
	}

	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile(path, counter.Count)
}

func (rd *rowDeltaProducer) writeDeleteManifest() (_ iceberg.ManifestFile, err error) {
	out, path, err := rd.newManifestOutput()
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(out, &err)

	counter := &internal.CountingWriter{W: out}
	currentSpec, err := rd.txn.meta.CurrentSpec()
	if err != nil {
		return nil, fmt.Errorf("could not get current partition spec: %w", err)
	}
	if currentSpec == nil {
		return nil, fmt.Errorf("current partition spec is nil")
	}

	// For delete files, use ManifestContentDeletes
	wr, err := iceberg.NewManifestWriter(rd.txn.meta.formatVersion, counter,
		*currentSpec, rd.txn.meta.CurrentSchema(), rd.snapshotID, iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(wr, &err)

	for _, df := range rd.addedDeleteFiles {
		if err := wr.Add(iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &rd.snapshotID, nil, nil, df)); err != nil {
			return nil, err
		}
	}

	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile(path, counter.Count)
}

// summary overrides base implementation to include delete file metrics
func (rd *rowDeltaProducer) summary(props iceberg.Properties) (Summary, error) {
	var ssc SnapshotSummaryCollector
	partitionSummaryLimit := rd.txn.meta.props.
		GetInt(WritePartitionSummaryLimitKey, WritePartitionSummaryLimitDefault)
	ssc.setPartitionSummaryLimit(partitionSummaryLimit)

	currentSchema := rd.txn.meta.CurrentSchema()
	partitionSpec, err := rd.txn.meta.CurrentSpec()
	if err != nil || partitionSpec == nil {
		return Summary{}, fmt.Errorf("could not get current partition spec: %w", err)
	}

	// Add data files
	for _, df := range rd.addedFiles {
		if err = ssc.addFile(df, currentSchema, *partitionSpec); err != nil {
			return Summary{}, err
		}
	}

	// Add delete files
	for _, df := range rd.addedDeleteFiles {
		if err = ssc.addFile(df, currentSchema, *partitionSpec); err != nil {
			return Summary{}, err
		}
	}

	var previousSnapshot *Snapshot
	if rd.parentSnapshotID > 0 {
		previousSnapshot, _ = rd.txn.meta.SnapshotByID(rd.parentSnapshotID)
	}

	var previousSummary iceberg.Properties
	if previousSnapshot != nil {
		previousSummary = previousSnapshot.Summary.Properties
	}

	summaryProps := ssc.build()
	maps.Copy(summaryProps, props)

	return updateSnapshotSummaries(Summary{
		Operation:  rd.op,
		Properties: summaryProps,
	}, previousSummary)
}

// commit creates the snapshot and returns updates/requirements
func (rd *rowDeltaProducer) commit() (_ []Update, _ []Requirement, err error) {
	newManifests, err := rd.manifests()
	if err != nil {
		return nil, nil, err
	}

	nextSequence := rd.txn.meta.nextSequenceNumber()
	summary, err := rd.summary(rd.snapshotProps)
	if err != nil {
		return nil, nil, err
	}

	fname := newManifestListFileName(rd.snapshotID, 0, rd.commitUuid)
	locProvider, err := rd.txn.tbl.LocationProvider()
	if err != nil {
		return nil, nil, err
	}

	manifestListFilePath := locProvider.NewMetadataLocation(fname)

	var parentSnapshot *int64
	if rd.parentSnapshotID > 0 {
		parentSnapshot = &rd.parentSnapshotID
	}

	firstRowID := int64(0)
	var addedRows int64

	out, err := rd.io.Create(manifestListFilePath)
	if err != nil {
		return nil, nil, err
	}
	defer internal.CheckedClose(out, &err)

	if rd.txn.meta.formatVersion == 3 {
		firstRowID = rd.txn.meta.NextRowID()
		writer, wrErr := iceberg.NewManifestListWriterV3(out, rd.snapshotID, nextSequence, firstRowID, parentSnapshot)
		if wrErr != nil {
			return nil, nil, wrErr
		}
		defer internal.CheckedClose(writer, &err)
		if wrErr = writer.AddManifests(newManifests); wrErr != nil {
			return nil, nil, wrErr
		}
		if writer.NextRowID() != nil {
			addedRows = *writer.NextRowID() - firstRowID
		}
	} else {
		err = iceberg.WriteManifestList(rd.txn.meta.formatVersion, out,
			rd.snapshotID, parentSnapshot, &nextSequence, firstRowID, newManifests)
		if err != nil {
			return nil, nil, err
		}
	}

	snapshot := Snapshot{
		SnapshotID:       rd.snapshotID,
		ParentSnapshotID: parentSnapshot,
		SequenceNumber:   nextSequence,
		ManifestList:     manifestListFilePath,
		Summary:          &summary,
		SchemaID:         &rd.txn.meta.currentSchemaID,
		TimestampMs:      time.Now().UnixMilli(),
	}
	if rd.txn.meta.formatVersion == 3 {
		snapshot.FirstRowID = &firstRowID
		snapshot.AddedRows = &addedRows
	}

	// Get the current snapshot ID for the target branch for the requirement
	var branchSnapshotID *int64
	if ref, ok := rd.txn.meta.refs[rd.targetBranch]; ok {
		branchSnapshotID = &ref.SnapshotID
	}

	return []Update{
			NewAddSnapshotUpdate(&snapshot),
			NewSetSnapshotRefUpdate(rd.targetBranch, rd.snapshotID, BranchRef, -1, -1, -1),
		}, []Requirement{
			AssertRefSnapshotID(rd.targetBranch, branchSnapshotID),
		}, nil
}
