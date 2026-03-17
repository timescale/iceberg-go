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
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
)

// RowDelta is a transaction operation that can add both data files and delete
// files atomically. This is the primary mechanism for implementing CDC-style
// updates using equality or position deletes.
type RowDelta struct {
	txn           *Transaction
	snapshotProps iceberg.Properties
	targetBranch  string

	addedDataFiles   []iceberg.DataFile
	addedDeleteFiles []iceberg.DataFile
}

// NewRowDelta creates a new RowDelta operation for the given transaction.
func NewRowDelta(txn *Transaction, snapshotProps iceberg.Properties) *RowDelta {
	return &RowDelta{
		txn:              txn,
		snapshotProps:    snapshotProps,
		targetBranch:     MainBranch,
		addedDataFiles:   make([]iceberg.DataFile, 0),
		addedDeleteFiles: make([]iceberg.DataFile, 0),
	}
}

// ToBranch sets the target branch for this row delta operation.
// If not called, the operation will be committed to the main branch.
func (r *RowDelta) ToBranch(branch string) *RowDelta {
	r.targetBranch = branch
	return r
}

// AddRows adds data files containing new rows to the transaction.
func (r *RowDelta) AddRows(files ...iceberg.DataFile) *RowDelta {
	for _, f := range files {
		if f.ContentType() != iceberg.EntryContentData {
			panic(fmt.Sprintf("AddRows requires data files, got content type %s", f.ContentType()))
		}
		r.addedDataFiles = append(r.addedDataFiles, f)
	}
	return r
}

// AddDeletes adds delete files (equality or position deletes) to the transaction.
func (r *RowDelta) AddDeletes(files ...iceberg.DataFile) *RowDelta {
	for _, f := range files {
		ct := f.ContentType()
		if ct != iceberg.EntryContentPosDeletes && ct != iceberg.EntryContentEqDeletes {
			panic(fmt.Sprintf("AddDeletes requires delete files, got content type %s", ct))
		}
		r.addedDeleteFiles = append(r.addedDeleteFiles, f)
	}
	return r
}

// Commit commits the row delta operation to the transaction.
func (r *RowDelta) Commit(ctx context.Context) error {
	if len(r.addedDataFiles) == 0 && len(r.addedDeleteFiles) == 0 {
		return nil // nothing to do
	}

	fs, err := r.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	producer := newRowDeltaProducer(r.txn, fs.(io.WriteFileIO), r.snapshotProps)

	// Set target branch if not main
	if r.targetBranch != MainBranch {
		producer.ToBranch(r.targetBranch)
	}

	for _, df := range r.addedDataFiles {
		producer.appendDataFile(df)
	}

	for _, df := range r.addedDeleteFiles {
		producer.appendDeleteFile(df)
	}

	updates, reqs, err := producer.commit()
	if err != nil {
		return err
	}

	return r.txn.apply(updates, reqs)
}

// RowDelta creates a new RowDelta operation for adding both data files and
// delete files atomically. This is used for CDC-style updates where you need
// to add new rows and mark old rows as deleted in the same commit.
func (t *Transaction) RowDelta(snapshotProps iceberg.Properties) *RowDelta {
	return NewRowDelta(t, snapshotProps)
}
