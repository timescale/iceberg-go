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

import "fmt"

// ManageSnapshots provides operations for managing branches and tags.
type ManageSnapshots struct {
	txn     *Transaction
	updates []Update
	reqs    []Requirement
	err     error
}

// NewManageSnapshots creates a new ManageSnapshots instance for the given transaction.
func NewManageSnapshots(txn *Transaction) *ManageSnapshots {
	return &ManageSnapshots{
		txn:     txn,
		updates: []Update{},
		reqs:    []Requirement{},
	}
}

// CreateBranch creates a new branch pointing to the given snapshot ID.
// If the branch already exists, this operation will fail.
func (m *ManageSnapshots) CreateBranch(name string, snapshotID int64) *ManageSnapshots {
	m.updates = append(m.updates, NewSetSnapshotRefUpdate(name, snapshotID, BranchRef, -1, -1, -1))
	return m
}

// CreateBranchFromCurrent creates a new branch pointing to the current snapshot.
// If there is no current snapshot, the error is deferred until Commit.
func (m *ManageSnapshots) CreateBranchFromCurrent(name string) *ManageSnapshots {
	snap := m.txn.meta.currentSnapshot()
	if snap == nil {
		m.err = fmt.Errorf("%w: cannot create branch from current snapshot: no current snapshot", ErrInvalidOperation)
		return m
	}
	m.updates = append(m.updates, NewSetSnapshotRefUpdate(name, snap.SnapshotID, BranchRef, -1, -1, -1))
	return m
}

// CreateTag creates a new tag pointing to the given snapshot ID.
func (m *ManageSnapshots) CreateTag(name string, snapshotID int64) *ManageSnapshots {
	m.updates = append(m.updates, NewSetSnapshotRefUpdate(name, snapshotID, TagRef, -1, -1, -1))
	return m
}

// RemoveBranch removes a branch by name.
// This does not delete the snapshots, only the branch reference.
func (m *ManageSnapshots) RemoveBranch(name string) *ManageSnapshots {
	if name == MainBranch {
		// Cannot remove main branch - this would leave the table in an invalid state
		return m
	}
	m.updates = append(m.updates, NewRemoveSnapshotRefUpdate(name))
	return m
}

// RemoveTag removes a tag by name.
func (m *ManageSnapshots) RemoveTag(name string) *ManageSnapshots {
	m.updates = append(m.updates, NewRemoveSnapshotRefUpdate(name))
	return m
}

// SetBranchSnapshot updates an existing branch to point to a different snapshot.
func (m *ManageSnapshots) SetBranchSnapshot(name string, snapshotID int64) *ManageSnapshots {
	m.updates = append(m.updates, NewSetSnapshotRefUpdate(name, snapshotID, BranchRef, -1, -1, -1))
	return m
}

// ReplaceBranch replaces one branch with another, pointing it to the same snapshot.
// This is useful for fast-forward merges.
func (m *ManageSnapshots) ReplaceBranch(from, to string) *ManageSnapshots {
	ref, ok := m.txn.meta.refs[from]
	if !ok {
		return m
	}
	m.updates = append(m.updates, NewSetSnapshotRefUpdate(to, ref.SnapshotID, BranchRef, -1, -1, -1))
	return m
}

// Commit applies all pending branch/tag operations to the transaction.
func (m *ManageSnapshots) Commit() error {
	if m.err != nil {
		return m.err
	}
	if len(m.updates) == 0 {
		return nil
	}
	return m.txn.apply(m.updates, m.reqs)
}

// Validate checks that all operations are valid without applying them.
func (m *ManageSnapshots) Validate() error {
	for _, u := range m.updates {
		switch upd := u.(type) {
		case *setSnapshotRefUpdate:
			// Verify snapshot exists
			if _, err := m.txn.meta.SnapshotByID(upd.SnapshotID); err != nil {
				return fmt.Errorf("snapshot %d not found: %w", upd.SnapshotID, err)
			}
		case *removeSnapshotRefUpdate:
			// Verify ref exists (warning only, removal of non-existent ref is a no-op)
			if _, ok := m.txn.meta.refs[upd.RefName]; !ok {
				// Not an error, just skip
			}
		}
	}
	return nil
}

// ManageSnapshots returns a ManageSnapshots instance for managing branches and tags.
func (t *Transaction) ManageSnapshots() *ManageSnapshots {
	return NewManageSnapshots(t)
}
