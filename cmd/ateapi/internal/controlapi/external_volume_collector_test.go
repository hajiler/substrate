// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
)

// recordingDeleter stands in for the ExternalVolume workflow, so a collector
// test can assert what was collected without provisioning anything.
type recordingDeleter struct {
	err     error
	deleted []resources.ExternalVolumeRef
}

func (d *recordingDeleter) DeleteExternalVolume(_ context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error) {
	if d.err != nil {
		return nil, d.err
	}
	d.deleted = append(d.deleted, volumeRef)
	return nil, nil
}

// advancePastGrace moves the collector's clock past the pending grace, which
// is how a test makes a stored volume look like wreckage: create_time is
// stamped by the store and immutable afterwards.
func advancePastGrace(c *ExternalVolumeCollector) {
	at := time.Now().Add(2 * externalVolumePendingGrace)
	c.now = func() time.Time { return at }
}

// TestExternalVolumeNeedsCollection covers the list-time filter, which decides
// which volumes are worth re-reading at all.
func TestExternalVolumeNeedsCollection(t *testing.T) {
	now := time.Now()
	pending := func(created time.Time) *ateapipb.ExternalVolume {
		return &ateapipb.ExternalVolume{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "v", CreateTime: timestamppb.New(created)},
			Status:   &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
		}
	}

	tests := []struct {
		name   string
		volume *ateapipb.ExternalVolume
		want   bool
	}{
		{
			name:   "a create that may still be running is left alone",
			volume: pending(now.Add(-time.Minute)),
			want:   false,
		},
		{
			name:   "a long-pending volume is wreckage",
			volume: pending(now.Add(-2 * externalVolumePendingGrace)),
			want:   true,
		},
		{
			name: "a ready volume nobody holds needs nothing",
			volume: &ateapipb.ExternalVolume{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "v"},
				Status:   &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY},
			},
			want: false,
		},
		{
			name: "a held volume is checked for dead holders",
			volume: &ateapipb.ExternalVolume{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "v"},
				Status: &ateapipb.ExternalVolumeStatus{
					State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY,
					Refs:  []*ateapipb.ActorRef{{ActorUid: "uid", ActorName: "a"}},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := externalVolumeNeedsCollection(tt.volume, now); got != tt.want {
				t.Errorf("externalVolumeNeedsCollection() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCollectOneDeletesStrandedVolume checks the create-that-died case: the
// pending row holds a name and may own a disk nobody can name, so it is
// deleted through the same workflow a client delete uses.
func TestCollectOneDeletesStrandedVolume(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	deleter := &recordingDeleter{}
	collector := NewExternalVolumeCollector(st, deleter)

	volumeRef := resources.ExternalVolumeRef{Atespace: "team-a", Name: "stranded"}
	storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: volumeRef.Atespace, Name: volumeRef.Name},
		ReclaimPolicy:    ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE,
		StorageClassName: "standard",
		Status:           &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
	})

	// A create that may still be running must survive the pass.
	if err := collector.collectOne(ctx, volumeRef); err != nil {
		t.Fatalf("collectOne: %v", err)
	}
	if len(deleter.deleted) != 0 {
		t.Fatalf("deleted %v, want a freshly reserved volume left alone", deleter.deleted)
	}

	advancePastGrace(collector)
	if err := collector.collectOne(ctx, volumeRef); err != nil {
		t.Fatalf("collectOne: %v", err)
	}
	if diff := cmp.Diff([]resources.ExternalVolumeRef{volumeRef}, deleter.deleted); diff != "" {
		t.Errorf("collected volumes mismatch (-want +got):\n%s", diff)
	}
}

// TestCollectOneDropsDeadRefs checks the reference side: a hold whose actor is
// gone, or whose name now belongs to a different actor, must not keep the
// volume undeletable, while a live actor's hold survives.
func TestCollectOneDropsDeadRefs(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	collector := NewExternalVolumeCollector(st, &recordingDeleter{})

	live := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "live"},
	})
	// An actor whose name was reused: it exists, but is not the one that took
	// the reference.
	reused := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "reused"},
	})

	volumeRef := resources.ExternalVolumeRef{Atespace: "team-a", Name: "shared"}
	storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: volumeRef.Atespace, Name: volumeRef.Name},
		ReclaimPolicy: ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN,
		VolumeId:      "disk/shared",
		VolumeType:    "mock",
		Status: &ateapipb.ExternalVolumeStatus{
			State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY,
			Refs: []*ateapipb.ActorRef{
				{ActorUid: live.GetMetadata().GetUid(), ActorName: "live"},
				{ActorUid: "00000000-0000-0000-0000-000000000000", ActorName: "deleted"},
				{ActorUid: "99999999-9999-9999-9999-999999999999", ActorName: "reused"},
			},
		},
	})

	if err := collector.collectOne(ctx, volumeRef); err != nil {
		t.Fatalf("collectOne: %v", err)
	}

	got, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		t.Fatalf("GetExternalVolume: %v", err)
	}
	want := []string{live.GetMetadata().GetUid()}
	if diff := cmp.Diff(want, refUIDs(got)); diff != "" {
		t.Errorf("refs mismatch (-want +got):\n%s", diff)
	}
	if reused.GetMetadata().GetUid() == "99999999-9999-9999-9999-999999999999" {
		t.Fatal("the reused actor kept the stale uid, so the test proves nothing")
	}
}

// TestCollectOneIgnoresDeletedVolume covers the ordinary race: the volume was
// deleted between the list that enqueued it and the pass that reads it back.
func TestCollectOneIgnoresDeletedVolume(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	collector := NewExternalVolumeCollector(st, &recordingDeleter{})

	err := collector.collectOne(ctx, resources.ExternalVolumeRef{Atespace: "team-a", Name: "gone"})
	if err != nil {
		t.Errorf("collectOne() = %v, want nil for a volume that no longer exists", err)
	}
}

// TestCollectOneReportsDeleteFailure checks that a collection that could not
// finish is reported, so the work item is requeued rather than forgotten.
func TestCollectOneReportsDeleteFailure(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	deleteErr := errors.New("driver unavailable")
	collector := NewExternalVolumeCollector(st, &recordingDeleter{err: deleteErr})

	volumeRef := resources.ExternalVolumeRef{Atespace: "team-a", Name: "stranded"}
	storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: volumeRef.Atespace, Name: volumeRef.Name},
		ReclaimPolicy:    ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE,
		StorageClassName: "standard",
		Status:           &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
	})
	advancePastGrace(collector)

	if err := collector.collectOne(ctx, volumeRef); !errors.Is(err, deleteErr) {
		t.Errorf("collectOne() = %v, want it to wrap %v", err, deleteErr)
	}
}

// TestCollectorCollectsThroughTheRealWorkflow wires the collector to the
// workflow it drives in production, so the two agree on what deleting a
// stranded volume means: the disk the failed create would have made is
// reclaimed, and the name is freed.
func TestCollectorCollectsThroughTheRealWorkflow(t *testing.T) {
	ctx := context.Background()
	workflow, st, plugin := newTestExternalVolumeWorkflow(t, corev1.PersistentVolumeReclaimDelete)
	collector := NewExternalVolumeCollector(st, workflow)

	volumeRef := resources.ExternalVolumeRef{Atespace: "team-a", Name: "stranded"}
	stranded := storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: volumeRef.Atespace, Name: volumeRef.Name},
		ReclaimPolicy:    ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE,
		StorageClassName: "standard",
		VolumeType:       "mock",
		Status:           &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
	})
	advancePastGrace(collector)

	if err := collector.collectOne(ctx, volumeRef); err != nil {
		t.Fatalf("collectOne: %v", err)
	}

	wantDeleted := []string{externalVolumeID(stranded.GetMetadata().GetUid())}
	if diff := cmp.Diff(wantDeleted, plugin.deletedIDs); diff != "" {
		t.Errorf("reclaimed disks mismatch (-want +got):\n%s", diff)
	}
	if _, err := st.GetExternalVolume(ctx, volumeRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetExternalVolume() = %v, want the name freed", err)
	}
}
