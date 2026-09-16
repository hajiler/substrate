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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

const (
	refTestAtespace = "team-refs"
	refTestActorUID = "11111111-1111-1111-1111-111111111111"
)

// borrowingTemplate is a template whose only volume borrows the named
// ExternalVolume under the mount name "shared-data".
func borrowingTemplate(volumeName string) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "tmpl"},
		Volumes: []*ateapipb.Volume{{
			Name:              "shared-data",
			ExternalVolumeRef: &ateapipb.ExternalVolumeRef{Name: volumeName},
		}},
	}
}

// refTestActor is an actor in refTestAtespace carrying the given volume list.
func refTestActor(volumes ...*ateapipb.ActorVolumeStatus) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "consumer", Uid: refTestActorUID},
		Status:   &ateapipb.ActorStatus{ActorVolumes: volumes},
	}
}

// mustStoreReadyVolume stores a READY ExternalVolume naming a disk, which is
// the only state an actor is allowed to claim.
func mustStoreReadyVolume(t *testing.T, ctx context.Context, st store.Interface, name string, refs ...*ateapipb.ActorRef) *ateapipb.ExternalVolume {
	t.Helper()
	return storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: name},
		ReclaimPolicy: ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN,
		VolumeId:      "disk/" + name,
		VolumeType:    "mock",
		VolumeContext: map[string]string{"type": "pd-ssd"},
		Status: &ateapipb.ExternalVolumeStatus{
			State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY,
			Refs:  refs,
		},
	})
}

func mustGetVolume(t *testing.T, ctx context.Context, st store.Interface, name string) *ateapipb.ExternalVolume {
	t.Helper()
	got, err := st.GetExternalVolume(ctx, resources.ExternalVolumeRef{Atespace: refTestAtespace, Name: name})
	if err != nil {
		t.Fatalf("GetExternalVolume(%q): %v", name, err)
	}
	return got
}

func refUIDs(volume *ateapipb.ExternalVolume) []string {
	uids := make([]string, 0, len(volume.GetStatus().GetRefs()))
	for _, ref := range volume.GetStatus().GetRefs() {
		uids = append(uids, ref.GetActorUid())
	}
	return uids
}

// TestClaimExternalVolumes_TakesRefAndRecordsMount is the whole claim: the
// actor appears in the volume's reference set, and gets back a mount carrying
// the handle it needs to attach, which it could not know from the template.
func TestClaimExternalVolumes_TakesRefAndRecordsMount(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreReadyVolume(t, ctx, st, "shared")

	got, err := claimExternalVolumes(ctx, st, refTestActor(), borrowingTemplate("shared"))
	if err != nil {
		t.Fatalf("claimExternalVolumes: %v", err)
	}

	want := []*ateapipb.ActorVolumeStatus{{
		VolumeName:         "shared-data",
		ExternalVolumeName: "shared",
		StorageVolumeId:    "disk/shared",
		VolumeType:         "mock",
		VolumeContext:      map[string]string{"type": "pd-ssd"},
		Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
	}}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("claimed volumes mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{refTestActorUID}, refUIDs(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs mismatch (-want +got):\n%s", diff)
	}
}

// TestClaimExternalVolumes_Idempotent covers the re-entered resume: the claim
// runs again and must not add the actor a second time.
func TestClaimExternalVolumes_Idempotent(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreReadyVolume(t, ctx, st, "shared")
	template := borrowingTemplate("shared")

	first, err := claimExternalVolumes(ctx, st, refTestActor(), template)
	if err != nil {
		t.Fatalf("first claimExternalVolumes: %v", err)
	}
	second, err := claimExternalVolumes(ctx, st, refTestActor(first...), template)
	if err != nil {
		t.Fatalf("second claimExternalVolumes: %v", err)
	}

	if diff := cmp.Diff(first, second, protocmp.Transform()); diff != "" {
		t.Errorf("re-claim changed the volume list (-first +second):\n%s", diff)
	}
	if diff := cmp.Diff([]string{refTestActorUID}, refUIDs(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs mismatch (-want +got):\n%s", diff)
	}
}

// TestClaimExternalVolumes_KeepsOwnedVolumes checks a template that mixes a
// borrowed volume with one the actor provisioned for itself: the owned entry
// must survive the claim untouched.
func TestClaimExternalVolumes_KeepsOwnedVolumes(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreReadyVolume(t, ctx, st, "shared")

	owned := &ateapipb.ActorVolumeStatus{
		VolumeName:      "scratch",
		StorageVolumeId: "disk/scratch",
		VolumeType:      "mock",
		Status:          ateapipb.ActorVolumeStatus_STATUS_CREATED,
	}
	got, err := claimExternalVolumes(ctx, st, refTestActor(owned), borrowingTemplate("shared"))
	if err != nil {
		t.Fatalf("claimExternalVolumes: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d volumes, want the owned one plus the borrowed one", len(got))
	}
	if diff := cmp.Diff(owned, got[0], protocmp.Transform()); diff != "" {
		t.Errorf("owned volume mismatch (-want +got):\n%s", diff)
	}
	if got[1].GetExternalVolumeName() != "shared" {
		t.Errorf("borrowed volume names %q, want %q", got[1].GetExternalVolumeName(), "shared")
	}
}

// TestClaimExternalVolumes_RefreshesStaleHandle covers a volume that was
// re-registered against a different disk while the actor was paused: the
// actor's entry must be replaced rather than duplicated, so it mounts the disk
// the record names now.
func TestClaimExternalVolumes_RefreshesStaleHandle(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreReadyVolume(t, ctx, st, "shared")

	stale := &ateapipb.ActorVolumeStatus{
		VolumeName:         "shared-data",
		ExternalVolumeName: "shared",
		StorageVolumeId:    "disk/gone",
		VolumeType:         "mock",
		Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
	}
	got, err := claimExternalVolumes(ctx, st, refTestActor(stale), borrowingTemplate("shared"))
	if err != nil {
		t.Fatalf("claimExternalVolumes: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d volumes, want the single entry replaced in place", len(got))
	}
	if got[0].GetStorageVolumeId() != "disk/shared" {
		t.Errorf("storage_volume_id = %q, want the handle the record names now", got[0].GetStorageVolumeId())
	}
}

// TestClaimExternalVolumes_Rejected covers the two states an actor may not
// resume against: a volume that is gone, and one whose disk does not exist yet.
func TestClaimExternalVolumes_Rejected(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "pending"},
		ReclaimPolicy:    ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE,
		StorageClassName: "standard",
		Status:           &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
	})

	for _, tt := range []struct {
		name       string
		volumeName string
	}{
		{name: "missing", volumeName: "absent"},
		{name: "not yet provisioned", volumeName: "pending"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := claimExternalVolumes(ctx, st, refTestActor(), borrowingTemplate(tt.volumeName))
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("claimExternalVolumes() = %v, want FailedPrecondition", err)
			}
		})
	}
}

// TestClaimExternalVolumes_AlreadyHeld is the exclusion rule: sharing is
// sequential, so a volume another actor is holding cannot be taken, and the
// refused claim must leave the holder's reference exactly as it found it.
func TestClaimExternalVolumes_AlreadyHeld(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	const holderUID = "22222222-2222-2222-2222-222222222222"
	mustStoreReadyVolume(t, ctx, st, "shared",
		&ateapipb.ActorRef{ActorUid: holderUID, ActorName: "producer"})

	_, err := claimExternalVolumes(ctx, st, refTestActor(), borrowingTemplate("shared"))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("claimExternalVolumes() = %v, want FailedPrecondition", err)
	}
	// The holder is named so the caller knows which actor to stop.
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "producer") {
		t.Errorf("error %q does not name the holding actor", msg)
	}
	if diff := cmp.Diff([]string{holderUID}, refUIDs(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs mismatch (-want +got):\n%s", diff)
	}

	// Once the holder gives the volume back, the next actor may take it.
	if err := releaseExternalVolume(ctx, st, resources.ExternalVolumeRef{Atespace: refTestAtespace, Name: "shared"}, holderUID); err != nil {
		t.Fatalf("releaseExternalVolume: %v", err)
	}
	if _, err := claimExternalVolumes(ctx, st, refTestActor(), borrowingTemplate("shared")); err != nil {
		t.Fatalf("claimExternalVolumes after release: %v", err)
	}
	if diff := cmp.Diff([]string{refTestActorUID}, refUIDs(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs mismatch after handoff (-want +got):\n%s", diff)
	}
}

// TestReleaseExternalVolumes_DropsOnlyThisActor checks that giving back one
// actor's reference leaves the references other actors hold in place.
func TestReleaseExternalVolumes_DropsOnlyThisActor(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	const otherUID = "22222222-2222-2222-2222-222222222222"
	mustStoreReadyVolume(t, ctx, st, "shared",
		&ateapipb.ActorRef{ActorUid: refTestActorUID, ActorName: "consumer"},
		&ateapipb.ActorRef{ActorUid: otherUID, ActorName: "other"})

	actor := refTestActor(&ateapipb.ActorVolumeStatus{
		VolumeName:         "shared-data",
		ExternalVolumeName: "shared",
		StorageVolumeId:    "disk/shared",
		VolumeType:         "mock",
		Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
	})
	if err := releaseExternalVolumes(ctx, st, actor); err != nil {
		t.Fatalf("releaseExternalVolumes: %v", err)
	}
	if diff := cmp.Diff([]string{otherUID}, refUIDs(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs mismatch (-want +got):\n%s", diff)
	}

	// Running it again is what a retried teardown does.
	if err := releaseExternalVolumes(ctx, st, actor); err != nil {
		t.Fatalf("second releaseExternalVolumes: %v", err)
	}
	if diff := cmp.Diff([]string{otherUID}, refUIDs(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs mismatch after re-release (-want +got):\n%s", diff)
	}
}

// TestCrashActorReleasesExternalVolumeRefs checks the path that used to leak a
// reference forever: a crashed actor is not running, so the volume it borrowed
// must become deletable again without waiting for the actor to be deleted.
func TestCrashActorReleasesExternalVolumeRefs(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)

	actorRef := resources.ActorRef{Atespace: refTestAtespace, Name: "consumer"}
	created := storetest.MustCreateActor(t, ctx, st, refTestActor())
	mustStoreReadyVolume(t, ctx, st, "shared", &ateapipb.ActorRef{ActorUid: created.GetMetadata().GetUid(), ActorName: "consumer"})
	mustUpdateActorStatus(t, ctx, st, created, func(s *ateapipb.ActorStatus) {
		s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		s.ActorVolumes = []*ateapipb.ActorVolumeStatus{{
			VolumeName:         "shared-data",
			ExternalVolumeName: "shared",
			StorageVolumeId:    "disk/shared",
			VolumeType:         "mock",
			Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
		}}
	})

	if err := crashActor(ctx, st, actorRef, "resume", "UNKNOWN"); err != nil {
		t.Fatalf("crashActor: %v", err)
	}
	if got := refUIDs(mustGetVolume(t, ctx, st, "shared")); len(got) != 0 {
		t.Errorf("refs = %v, want the crashed actor's hold released", got)
	}
}

// TestReleaseExternalVolumes_IgnoresOwnedAndMissing checks the two cases a
// teardown must not fail on: a volume the actor owns outright, which holds no
// reference, and a borrowed volume that has already been deleted.
func TestReleaseExternalVolumes_IgnoresOwnedAndMissing(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	storetest.MustCreateAtespace(t, ctx, st, refTestAtespace)

	actor := refTestActor(
		&ateapipb.ActorVolumeStatus{
			VolumeName:      "scratch",
			StorageVolumeId: "disk/scratch",
			VolumeType:      "mock",
			Status:          ateapipb.ActorVolumeStatus_STATUS_CREATED,
		},
		&ateapipb.ActorVolumeStatus{
			VolumeName:         "shared-data",
			ExternalVolumeName: "gone",
			StorageVolumeId:    "disk/gone",
			VolumeType:         "mock",
			Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
		})
	if err := releaseExternalVolumes(ctx, st, actor); err != nil {
		t.Errorf("releaseExternalVolumes() = %v, want nil", err)
	}
}
