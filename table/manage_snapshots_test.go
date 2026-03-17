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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManageSnapshotsCreateBranch(t *testing.T) {
	builder := &MetadataBuilder{
		formatVersion: 2,
		refs: map[string]SnapshotRef{
			MainBranch: {SnapshotID: 1234567890, SnapshotRefType: BranchRef},
		},
		snapshotList: []Snapshot{
			{SnapshotID: 1234567890, SequenceNumber: 1},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	ms := NewManageSnapshots(txn)

	// Test CreateBranch
	ms.CreateBranch("test-branch", 1234567890)
	assert.Len(t, ms.updates, 1)

	update, ok := ms.updates[0].(*setSnapshotRefUpdate)
	require.True(t, ok)
	assert.Equal(t, "test-branch", update.RefName)
	assert.Equal(t, int64(1234567890), update.SnapshotID)
	assert.Equal(t, BranchRef, update.RefType)
}

func TestManageSnapshotsCreateBranchFromCurrent(t *testing.T) {
	currentSnapshotID := int64(9876543210)
	builder := &MetadataBuilder{
		formatVersion:     2,
		currentSnapshotID: &currentSnapshotID,
		refs: map[string]SnapshotRef{
			MainBranch: {SnapshotID: currentSnapshotID, SnapshotRefType: BranchRef},
		},
		snapshotList: []Snapshot{
			{SnapshotID: currentSnapshotID, SequenceNumber: 1},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	ms := NewManageSnapshots(txn)
	ms.CreateBranchFromCurrent("feature-branch")

	require.Len(t, ms.updates, 1)
	update, ok := ms.updates[0].(*setSnapshotRefUpdate)
	require.True(t, ok)
	assert.Equal(t, "feature-branch", update.RefName)
	assert.Equal(t, currentSnapshotID, update.SnapshotID)
}

func TestManageSnapshotsRemoveBranch(t *testing.T) {
	builder := &MetadataBuilder{
		formatVersion: 2,
		refs: map[string]SnapshotRef{
			MainBranch:    {SnapshotID: 1, SnapshotRefType: BranchRef},
			"test-branch": {SnapshotID: 2, SnapshotRefType: BranchRef},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	ms := NewManageSnapshots(txn)

	// Test removing a non-main branch
	ms.RemoveBranch("test-branch")
	require.Len(t, ms.updates, 1)

	update, ok := ms.updates[0].(*removeSnapshotRefUpdate)
	require.True(t, ok)
	assert.Equal(t, "test-branch", update.RefName)
}

func TestManageSnapshotsCannotRemoveMainBranch(t *testing.T) {
	builder := &MetadataBuilder{
		formatVersion: 2,
		refs: map[string]SnapshotRef{
			MainBranch: {SnapshotID: 1, SnapshotRefType: BranchRef},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	ms := NewManageSnapshots(txn)

	// Attempting to remove main branch should be a no-op
	ms.RemoveBranch(MainBranch)
	assert.Len(t, ms.updates, 0)
}

func TestManageSnapshotsCreateTag(t *testing.T) {
	builder := &MetadataBuilder{
		formatVersion: 2,
		refs: map[string]SnapshotRef{
			MainBranch: {SnapshotID: 1, SnapshotRefType: BranchRef},
		},
		snapshotList: []Snapshot{
			{SnapshotID: 1, SequenceNumber: 1},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	ms := NewManageSnapshots(txn)
	ms.CreateTag("v1.0.0", 1)

	require.Len(t, ms.updates, 1)
	update, ok := ms.updates[0].(*setSnapshotRefUpdate)
	require.True(t, ok)
	assert.Equal(t, "v1.0.0", update.RefName)
	assert.Equal(t, int64(1), update.SnapshotID)
	assert.Equal(t, TagRef, update.RefType)
}

func TestManageSnapshotsChaining(t *testing.T) {
	builder := &MetadataBuilder{
		formatVersion: 2,
		refs: map[string]SnapshotRef{
			MainBranch:   {SnapshotID: 1, SnapshotRefType: BranchRef},
			"old-branch": {SnapshotID: 2, SnapshotRefType: BranchRef},
		},
		snapshotList: []Snapshot{
			{SnapshotID: 1, SequenceNumber: 1},
			{SnapshotID: 2, SequenceNumber: 2},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	ms := NewManageSnapshots(txn)

	// Test method chaining
	ms.CreateBranch("new-branch", 1).
		CreateTag("v1.0", 1).
		RemoveBranch("old-branch")

	assert.Len(t, ms.updates, 3)
}

func TestManageSnapshotsValidate(t *testing.T) {
	builder := &MetadataBuilder{
		formatVersion: 2,
		refs: map[string]SnapshotRef{
			MainBranch: {SnapshotID: 1, SnapshotRefType: BranchRef},
		},
		snapshotList: []Snapshot{
			{SnapshotID: 1, SequenceNumber: 1},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	t.Run("valid snapshot reference", func(t *testing.T) {
		ms := NewManageSnapshots(txn)
		ms.CreateBranch("test", 1)
		err := ms.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid snapshot reference", func(t *testing.T) {
		ms := NewManageSnapshots(txn)
		ms.CreateBranch("test", 99999) // Non-existent snapshot
		err := ms.Validate()
		assert.Error(t, err)
	})
}

func TestSnapshotProducerToBranch(t *testing.T) {
	currentSnapshotID := int64(100)
	builder := &MetadataBuilder{
		formatVersion:     2,
		currentSnapshotID: &currentSnapshotID,
		refs: map[string]SnapshotRef{
			MainBranch:     {SnapshotID: 100, SnapshotRefType: BranchRef},
			"other-branch": {SnapshotID: 200, SnapshotRefType: BranchRef},
		},
		snapshotList: []Snapshot{
			{SnapshotID: 100, SequenceNumber: 1},
			{SnapshotID: 200, SequenceNumber: 2},
		},
	}

	txn := &Transaction{
		meta: builder,
	}

	// Create a snapshot producer via the snapshotUpdate helper
	su := snapshotUpdate{txn: txn}
	producer := su.fastAppend()

	// Default should be main branch
	assert.Equal(t, MainBranch, producer.targetBranch)
	assert.Equal(t, int64(100), producer.parentSnapshotID)

	// After ToBranch, should update both target and parent
	producer.ToBranch("other-branch")
	assert.Equal(t, "other-branch", producer.targetBranch)
	assert.Equal(t, int64(200), producer.parentSnapshotID)

	// ToBranch to non-existent branch should set parent to -1
	producer.ToBranch("new-branch")
	assert.Equal(t, "new-branch", producer.targetBranch)
	assert.Equal(t, int64(-1), producer.parentSnapshotID)
}
