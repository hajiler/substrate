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

package functionaltest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// sharedVolumePlugin records what the control plane asked the storage system
// for, so a test driving the RPCs from outside can still see which disk was
// provisioned, attached, and reclaimed.
type sharedVolumePlugin struct {
	volume.VolumePluginControlPlane

	mu       sync.Mutex
	created  []string
	deleted  []string
	attached []string
	detached []string
}

func (p *sharedVolumePlugin) CreateVolume(_ context.Context, name, _, _ string, parameters map[string]string) (string, map[string]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.created = append(p.created, name)
	return "disk/" + name, parameters, nil
}

func (p *sharedVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted = append(p.deleted, volumeID)
	return nil
}

func (p *sharedVolumePlugin) AttachVolume(_ context.Context, volumeID, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attached = append(p.attached, volumeID+"@"+node)
	return nil
}

func (p *sharedVolumePlugin) DetachVolume(_ context.Context, volumeID, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.detached = append(p.detached, volumeID+"@"+node)
	return nil
}

func (p *sharedVolumePlugin) snapshot() (created, deleted, attached, detached []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.created...), append([]string(nil), p.deleted...),
		append([]string(nil), p.attached...), append([]string(nil), p.detached...)
}

// setupExternalVolumeTest is setupTest with a recording plugin behind the mock
// driver the "standard" and "fast" StorageClasses name.
func setupExternalVolumeTest(t *testing.T, ns string) (*testContext, *sharedVolumePlugin) {
	t.Helper()
	plugin := &sharedVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	return tc, plugin
}

// createExternalVolume provisions an ExternalVolume from the "standard" class
// through the RPC, and fails the test if that does not finish.
func createExternalVolume(t *testing.T, tc *testContext, name string, reclaim ateapipb.ReclaimPolicy) *ateapipb.ExternalVolume {
	t.Helper()
	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			StorageClassName: "standard",
			Capacity:         "10Gi",
			ReclaimPolicy:    reclaim,
		},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume(%s) failed: %v", name, err)
	}
	if got := created.GetStatus().GetState(); got != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
		t.Fatalf("CreateExternalVolume(%s) left the volume in %v, want READY", name, got)
	}
	return created
}

// refNames lists the actors currently holding volumeName, in the order the
// volume records them.
func refNames(t *testing.T, tc *testContext, volumeName string) []string {
	t.Helper()
	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapipb.GetExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: volumeName},
	})
	if err != nil {
		t.Fatalf("GetExternalVolume(%s) failed: %v", volumeName, err)
	}
	var names []string
	for _, ref := range stored.GetStatus().GetRefs() {
		names = append(names, ref.GetActorName())
	}
	return names
}

// borrowedMount returns the actor's recorded mount of the volume its template
// borrows under volumeName.
func borrowedMount(t *testing.T, tc *testContext, actorName, volumeName string) *ateapipb.ActorVolumeStatus {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", actorName, err)
	}
	for _, vol := range actor.GetStatus().GetActorVolumes() {
		if vol.GetVolumeName() == volumeName {
			return vol
		}
	}
	return nil
}

// TestCreateExternalVolume_Provisioned covers the ordinary create: a class name
// and a size produce a disk, and the record that names it is what Get and List
// hand back.
func TestCreateExternalVolume_Provisioned(t *testing.T) {
	ns := namespaceForTest("ns-extvol-provisioned")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE)

	// The disk is named after the record's own UID, which only exists once the
	// name is reserved.
	wantDiskName := "substrate-" + created.GetMetadata().GetUid()
	gotCreated, _, _, _ := plugin.snapshot()
	if diff := cmp.Diff([]string{wantDiskName}, gotCreated); diff != "" {
		t.Errorf("provisioned disks mismatch (-want +got):\n%s", diff)
	}
	if got, want := created.GetVolumeId(), "disk/"+wantDiskName; got != want {
		t.Errorf("volume_id = %q, want %q", got, want)
	}
	if got, want := created.GetVolumeType(), "substrate.io/mock"; got != want {
		t.Errorf("volume_type = %q, want %q, the provisioner of the class it was made from", got, want)
	}

	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapipb.GetExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"},
	})
	if err != nil {
		t.Fatalf("GetExternalVolume failed: %v", err)
	}
	if diff := cmp.Diff(created, stored, protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolume mismatch (-want +got):\n%s", diff)
	}

	listed, err := tc.client.ListExternalVolumes(context.Background(), &ateapipb.ListExternalVolumesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListExternalVolumes failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.ExternalVolume{created}, listed.GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("ListExternalVolumes mismatch (-want +got):\n%s", diff)
	}
}

// TestCreateExternalVolume_Registered covers registering a disk that already
// exists, which is how an admin hands Substrate storage it did not make: the
// record is ready immediately and nothing is provisioned.
func TestCreateExternalVolume_Registered(t *testing.T) {
	ns := namespaceForTest("ns-extvol-registered")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:   &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "adopted"},
			VolumeId:   "disk/made-elsewhere",
			VolumeType: "substrate.io/mock",
		},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume failed: %v", err)
	}
	if got := created.GetStatus().GetState(); got != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
		t.Errorf("state = %v, want READY: a disk that already exists has nothing left to do", got)
	}
	// A disk Substrate did not make is retained unless the request says
	// otherwise.
	if got := created.GetReclaimPolicy(); got != ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN {
		t.Errorf("reclaim_policy = %v, want RETAIN", got)
	}
	if gotCreated, _, _, _ := plugin.snapshot(); len(gotCreated) != 0 {
		t.Errorf("provisioned %v, want nothing provisioned for a volume that already exists", gotCreated)
	}
}

// TestCreateExternalVolume_ReusedName checks that a name already taken is
// reported as taken: a create that lands on an existing volume would hand its
// caller somebody else's disk.
func TestCreateExternalVolume_ReusedName(t *testing.T) {
	ns := namespaceForTest("ns-extvol-reused-name")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	first := createExternalVolume(t, tc, "shared", ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE)

	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "shared"},
			StorageClassName: "fast",
			Capacity:         "20Gi",
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists,
		fmt.Sprintf("ExternalVolume %s/shared already exists; delete it and create it again to retry", testAtespace))

	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapipb.GetExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"},
	})
	if err != nil {
		t.Fatalf("GetExternalVolume failed: %v", err)
	}
	if diff := cmp.Diff(first, stored, protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}
}

// TestUpdateExternalVolume covers what an update may and may not change: the
// reclaim policy is a decision the operator can revisit, where the storage is
// is not.
func TestUpdateExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-update")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN)

	// Echo the volume back with only the policy changed.
	toUpdate := func(in *ateapipb.ExternalVolume) *ateapipb.ExternalVolume {
		return &ateapipb.ExternalVolume{
			Metadata:         in.GetMetadata(),
			StorageClassName: in.GetStorageClassName(),
			Capacity:         in.GetCapacity(),
			VolumeId:         in.GetVolumeId(),
			VolumeType:       in.GetVolumeType(),
			VolumeContext:    in.GetVolumeContext(),
			ReclaimPolicy:    in.GetReclaimPolicy(),
		}
	}

	req := toUpdate(created)
	req.ReclaimPolicy = ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE
	updated, err := tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	if err != nil {
		t.Fatalf("UpdateExternalVolume failed: %v", err)
	}
	if got := updated.GetReclaimPolicy(); got != ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE {
		t.Errorf("reclaim_policy = %v, want DELETE", got)
	}
	if got, want := updated.GetVolumeId(), created.GetVolumeId(); got != want {
		t.Errorf("volume_id = %q, want it unchanged at %q", got, want)
	}

	// Repointing the record at another disk would hand every actor referencing
	// it different storage.
	req = toUpdate(updated)
	req.VolumeId = "disk/somewhere-else"
	_, err = tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")
}

// TestDeleteExternalVolume_Borrowed checks the guard that makes sharing safe:
// a volume an actor is holding cannot be deleted, and once the actor gives it
// back the delete reclaims the disk.
func TestDeleteExternalVolume_Borrowed(t *testing.T) {
	ns := namespaceForTest("ns-extvol-borrowed")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "shared", ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", ExternalVolumeRef: &ateapipb.ExternalVolumeRef{Name: "shared"}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	ctx := context.Background()
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "borrower"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	// Nothing is claimed until the actor runs: a suspended actor holds no
	// volume.
	if got := refNames(t, tc, "shared"); len(got) != 0 {
		t.Errorf("refs after create = %v, want none until the actor resumes", got)
	}

	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if diff := cmp.Diff([]string{"borrower"}, refNames(t, tc, "shared")); diff != "" {
		t.Errorf("refs after resume mismatch (-want +got):\n%s", diff)
	}

	mount := borrowedMount(t, tc, "borrower", "work")
	if mount == nil {
		t.Fatalf("the actor recorded no mount for the volume it borrows")
	}
	want := &ateapipb.ActorVolumeStatus{
		VolumeName:         "work",
		ExternalVolumeName: "shared",
		StorageVolumeId:    shared.GetVolumeId(),
		VolumeType:         shared.GetVolumeType(),
		Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
	}
	if diff := cmp.Diff(want, mount, protocmp.Transform()); diff != "" {
		t.Errorf("borrowed mount mismatch (-want +got):\n%s", diff)
	}

	_, err := tc.client.DeleteExternalVolume(ctx, &ateapipb.DeleteExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"},
	})
	assertGrpcError(t, err, codes.FailedPrecondition,
		fmt.Sprintf("ExternalVolume %s/shared is still referenced by 1 actor(s), starting with %q", testAtespace, "borrower"))

	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if got := refNames(t, tc, "shared"); len(got) != 0 {
		t.Errorf("refs after suspend = %v, want none: a suspended actor is not running", got)
	}

	if _, err := tc.client.DeleteExternalVolume(ctx, &ateapipb.DeleteExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"},
	}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}

	_, deleted, attached, detached := plugin.snapshot()
	if diff := cmp.Diff([]string{shared.GetVolumeId()}, deleted); diff != "" {
		t.Errorf("reclaimed disks mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1"}, attached); diff != "" {
		t.Errorf("attached disks mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1"}, detached); diff != "" {
		t.Errorf("detached disks mismatch (-want +got):\n%s", diff)
	}

	_, err = tc.client.GetExternalVolume(ctx, &ateapipb.GetExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"},
	})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/shared not found", testAtespace))
}

// TestExternalVolume_SequentialHandoff is the producer/consumer case the shared
// volume exists for: one actor writes and stops, the next one starts and reads
// the same disk. Sharing is sequential in this milestone, so what matters is
// that the reference moves with the running actor and both actors are handed
// the same storage.
func TestExternalVolume_SequentialHandoff(t *testing.T) {
	ns := namespaceForTest("ns-extvol-handoff")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "handoff", ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", ExternalVolumeRef: &ateapipb.ExternalVolumeRef{Name: "handoff"}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	ctx := context.Background()
	for _, name := range []string{"producer", "consumer"} {
		if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}}); err != nil {
			t.Fatalf("CreateActor(%s) failed: %v", name, err)
		}
	}

	// The producer runs, holding the volume for as long as it does.
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"},
	}); err != nil {
		t.Fatalf("ResumeActor(producer) failed: %v", err)
	}
	if diff := cmp.Diff([]string{"producer"}, refNames(t, tc, "handoff")); diff != "" {
		t.Errorf("refs while the producer runs mismatch (-want +got):\n%s", diff)
	}

	// It stops, which is the handoff: the disk leaves the node and the
	// reference is given back.
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"},
	}); err != nil {
		t.Fatalf("SuspendActor(producer) failed: %v", err)
	}
	if got := refNames(t, tc, "handoff"); len(got) != 0 {
		t.Errorf("refs after the producer stops = %v, want none", got)
	}

	// The consumer takes it over and is handed the same storage.
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"},
	}); err != nil {
		t.Fatalf("ResumeActor(consumer) failed: %v", err)
	}
	if diff := cmp.Diff([]string{"consumer"}, refNames(t, tc, "handoff")); diff != "" {
		t.Errorf("refs while the consumer runs mismatch (-want +got):\n%s", diff)
	}

	for _, name := range []string{"producer", "consumer"} {
		mount := borrowedMount(t, tc, name, "work")
		if got, want := mount.GetStorageVolumeId(), shared.GetVolumeId(); got != want {
			t.Errorf("actor %s mounted %q, want the shared disk %q", name, got, want)
		}
	}

	// The disk was staged for each actor in turn, and never for two at once.
	created, deleted, attached, detached := plugin.snapshot()
	wantAttached := []string{shared.GetVolumeId() + "@node1", shared.GetVolumeId() + "@node1"}
	if diff := cmp.Diff(wantAttached, attached); diff != "" {
		t.Errorf("attached disks mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1"}, detached); diff != "" {
		t.Errorf("detached disks mismatch (-want +got):\n%s", diff)
	}
	// One disk, made once by the create and never by an actor.
	if diff := cmp.Diff([]string{"substrate-" + shared.GetMetadata().GetUid()}, created); diff != "" {
		t.Errorf("provisioned disks mismatch (-want +got):\n%s", diff)
	}
	if len(deleted) != 0 {
		t.Errorf("deleted %v, want the shared disk left alone: the actors only borrowed it", deleted)
	}
}

// TestDeleteActor_LeavesBorrowedVolume checks that an actor's delete does not
// take the storage it borrowed with it: the disk belongs to the ExternalVolume,
// which outlives every actor that mounts it.
func TestDeleteActor_LeavesBorrowedVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-delete-actor")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "shared", ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", ExternalVolumeRef: &ateapipb.ExternalVolumeRef{Name: "shared"}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	ctx := context.Background()
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "borrower"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if got := refNames(t, tc, "shared"); len(got) != 0 {
		t.Errorf("refs after the actor is deleted = %v, want none", got)
	}
	if _, deleted, _, _ := plugin.snapshot(); len(deleted) != 0 {
		t.Errorf("deleted %v, want the borrowed disk left to the ExternalVolume that owns it", deleted)
	}

	// The volume outlived the actor and is deletable again. Its policy retains
	// the disk, so nothing is reclaimed even then.
	if _, err := tc.client.DeleteExternalVolume(ctx, &ateapipb.DeleteExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"},
	}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
	if _, deleted, _, _ := plugin.snapshot(); len(deleted) != 0 {
		t.Errorf("reclaimed %v, want RETAIN to leave the disk %q in place", deleted, shared.GetVolumeId())
	}
}

// TestResumeActor_ExternalVolumeMissing checks that borrowing a volume that is
// not there fails the resume rather than starting the actor without its
// storage.
func TestResumeActor_ExternalVolumeMissing(t *testing.T) {
	ns := namespaceForTest("ns-extvol-missing")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", ExternalVolumeRef: &ateapipb.ExternalVolumeRef{Name: "nonexistent"}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	ctx := context.Background()
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "borrower"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	_, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition,
		fmt.Sprintf("ExternalVolume %s/nonexistent not found", testAtespace))

	actor, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state = %v, want it left SUSPENDED", got)
	}
}
