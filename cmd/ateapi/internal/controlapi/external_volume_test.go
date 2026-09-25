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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// recordingVolumePlugin answers CreateVolume with a handle derived from the
// name it was asked for, and records both calls so a test can assert on what
// the workflow asked the storage system to do.
type recordingVolumePlugin struct {
	volume.VolumePluginControlPlane

	createErr  error
	deleteErr  error
	createdIDs []string
	deletedIDs []string
}

func (p *recordingVolumePlugin) CreateVolume(_ context.Context, name, capacity, driverName string, _ map[string]string) (string, map[string]string, error) {
	if p.createErr != nil {
		return "", nil, p.createErr
	}
	p.createdIDs = append(p.createdIDs, name)
	return "disk/" + name, map[string]string{"capacity": capacity, "driver": driverName}, nil
}

func (p *recordingVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	if p.deleteErr != nil {
		return p.deleteErr
	}
	p.deletedIDs = append(p.deletedIDs, volumeID)
	return nil
}

// newTestExternalVolumeWorkflow builds an ExternalVolumeWorkflow over a real
// store, with one StorageClass named "standard" whose reclaim policy the caller
// chooses, and a plugin registered under that class's provisioner.
func newTestExternalVolumeWorkflow(t *testing.T) (*ExternalVolumeWorkflow, store.Interface, *recordingVolumePlugin) {
	t.Helper()
	persistence := newTestPersistence(t)
	plugin := &recordingVolumePlugin{}
	scLister := &fakeStorageClassLister{
		storageClasses: map[string]*storagev1.StorageClass{
			"standard": {
				ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
				Provisioner: "mock",
				Parameters:  map[string]string{"type": "pd-ssd"},
			},
		},
	}
	registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}}
	return NewExternalVolumeWorkflow(persistence, scLister, registry), persistence, plugin
}

// TestCreateExternalVolume_Provisioned checks the whole create: the record is
// reserved, the disk is asked for under an ID derived from the record's UID,
// and the handle the driver returned is stamped back.
func TestCreateExternalVolume_Provisioned(t *testing.T) {
	ctx := context.Background()
	w, _, plugin := newTestExternalVolumeWorkflow(t)

	got, err := w.CreateExternalVolume(ctx, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		StorageClassName: "standard",
		Capacity:         "10Gi",
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}

	wantID := externalVolumeID(got.GetMetadata().GetUid())
	if diff := cmp.Diff([]string{wantID}, plugin.createdIDs); diff != "" {
		t.Errorf("provisioned volume names mismatch (-want +got):\n%s", diff)
	}
	if want := "disk/" + wantID; got.GetVolumeId() != want {
		t.Errorf("volume_id = %q, want the handle the driver returned %q", got.GetVolumeId(), want)
	}
	if got.GetVolumeType() != "mock" {
		t.Errorf("volume_type = %q, want the class's provisioner %q", got.GetVolumeType(), "mock")
	}
	if want := ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR; got.GetDeleteTrigger() != want {
		t.Errorf("delete_trigger = %v, want %v", got.GetDeleteTrigger(), want)
	}
	if want := ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY; got.GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), want)
	}
	if got.GetVolumeContext()["capacity"] != "10Gi" {
		t.Errorf("volume_context = %v, want the context the driver returned for a 10Gi disk", got.GetVolumeContext())
	}
}

func TestCreateExternalVolume_Registered(t *testing.T) {
	ctx := context.Background()
	w, _, plugin := newTestExternalVolumeWorkflow(t)

	got, err := w.CreateExternalVolume(ctx, &ateapipb.ExternalVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "adopted"},
		VolumeId:      "projects/p/zones/z/disks/preexisting",
		VolumeType:    "mock",
		VolumeContext: map[string]string{"fsType": "ext4"},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}

	if len(plugin.createdIDs) != 0 {
		t.Errorf("provisioned %v, want nothing provisioned for a volume that already exists", plugin.createdIDs)
	}
	if want := "projects/p/zones/z/disks/preexisting"; got.GetVolumeId() != want {
		t.Errorf("volume_id = %q, want the supplied %q", got.GetVolumeId(), want)
	}
	if want := ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY; got.GetStatus().GetState() != want {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), want)
	}
	if want := ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL; got.GetDeleteTrigger() != want {
		t.Errorf("delete_trigger = %v, want %v", got.GetDeleteTrigger(), want)
	}
}

func TestCreateExternalVolume_Errors(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		volume   *ateapipb.ExternalVolume
		wantCode codes.Code
	}{
		{
			name: "unknown storage class",
			volume: &ateapipb.ExternalVolume{
				Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "vol"},
				StorageClassName: "nonexistent",
				Capacity:         "1Gi",
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "unknown atespace",
			volume: &ateapipb.ExternalVolume{
				Metadata:         &ateapipb.ResourceMetadata{Atespace: "no-such-space", Name: "vol"},
				StorageClassName: "standard",
				Capacity:         "1Gi",
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "no source at all",
			volume: &ateapipb.ExternalVolume{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "vol"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, _, _ := newTestExternalVolumeWorkflow(t)
			_, err := w.CreateExternalVolume(ctx, tc.volume)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("CreateExternalVolume error = %v (code %v), want code %v", err, got, tc.wantCode)
			}
		})
	}
}

func TestCreateExternalVolume_ReusedName(t *testing.T) {
	ctx := context.Background()
	w, _, plugin := newTestExternalVolumeWorkflow(t)

	toCreate := func() *ateapipb.ExternalVolume {
		return &ateapipb.ExternalVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
			StorageClassName: "standard",
			Capacity:         "10Gi",
		}
	}
	if _, err := w.CreateExternalVolume(ctx, toCreate()); err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}
	_, err := w.CreateExternalVolume(ctx, toCreate())
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("second CreateExternalVolume error = %v (code %v), want code %v", err, got, codes.AlreadyExists)
	}
	if len(plugin.createdIDs) != 1 {
		t.Errorf("provisioned %v, want only the first create to reach the storage system", plugin.createdIDs)
	}
}

func TestDeleteExternalVolume(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestExternalVolumeWorkflow(t)
	created, err := w.CreateExternalVolume(ctx, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		StorageClassName: "standard",
		Capacity:         "10Gi",
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(created)

	if _, err := w.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{Version: created.GetMetadata().GetVersion() + 1}); status.Code(err) != codes.Aborted {
		t.Fatalf("DeleteExternalVolume with a stale version error = %v, want code %v", err, codes.Aborted)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Fatalf("collected %v before the precondition check passed", plugin.deletedIDs)
	}

	if _, err := w.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{UID: created.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteExternalVolume: %v", err)
	}
	if diff := cmp.Diff([]string{created.GetVolumeId()}, plugin.deletedIDs); diff != "" {
		t.Errorf("collected disks mismatch (-want +got):\n%s", diff)
	}
	if _, err := persistence.GetExternalVolume(ctx, volumeRef); err == nil {
		t.Errorf("GetExternalVolume after delete succeeded, want the row gone")
	}
}

func TestDeleteExternalVolume_StillReferenced(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestExternalVolumeWorkflow(t)

	created, err := w.CreateExternalVolume(ctx, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		StorageClassName: "standard",
		Capacity:         "10Gi",
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(created)
	if _, err := persistence.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.ExternalVolume) error {
		toUpdate.Status.Refs = []*ateapipb.ActorRef{{ActorUid: "uid-1", ActorName: "producer"}}
		return nil
	}); err != nil {
		t.Fatalf("UpdateExternalVolume: %v", err)
	}

	_, err = w.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("DeleteExternalVolume error = %v (code %v), want code %v", err, got, codes.FailedPrecondition)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Errorf("collected %v, want the disk left alone while an actor holds it", plugin.deletedIDs)
	}
}

func TestDeleteExternalVolume_Pending(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestExternalVolumeWorkflow(t)

	pending, err := persistence.CreateExternalVolume(ctx, &ateapipb.ExternalVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "stranded"},
		StorageClassName: "standard",
		VolumeType:       "mock",
		Status:           &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}

	if _, err := w.DeleteExternalVolume(ctx, resources.ExternalVolumeRefFromExternalVolume(pending), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteExternalVolume: %v", err)
	}
	want := []string{externalVolumeID(pending.GetMetadata().GetUid())}
	if diff := cmp.Diff(want, plugin.deletedIDs); diff != "" {
		t.Errorf("collected disks mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExternalVolume_NotFound(t *testing.T) {
	ctx := context.Background()
	w, _, _ := newTestExternalVolumeWorkflow(t)

	_, err := w.DeleteExternalVolume(ctx, resources.ExternalVolumeRef{Atespace: "team-a", Name: "nope"}, store.DeletePreconditions{})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("DeleteExternalVolume error = %v (code %v), want code %v", err, got, codes.NotFound)
	}
}

func TestValidateCustomCreateExternalVolumeRequest(t *testing.T) {
	tests := []struct {
		name       string
		volume     *ateapipb.ExternalVolume
		wantFields []string
	}{
		{
			name:   "provisioning from a class",
			volume: &ateapipb.ExternalVolume{StorageClassName: "standard", Capacity: "10Gi"},
		},
		{
			name:   "registering an existing volume",
			volume: &ateapipb.ExternalVolume{VolumeId: "disk-1", VolumeType: "mock"},
		},
		{
			name:   "registering an existing volume under a named class",
			volume: &ateapipb.ExternalVolume{VolumeId: "disk-1", StorageClassName: "standard"},
		},
		{
			name:       "neither a class nor an ID",
			volume:     &ateapipb.ExternalVolume{},
			wantFields: []string{"external_volume.storage_class_name"},
		},
		{
			name:       "a size for a volume that already exists",
			volume:     &ateapipb.ExternalVolume{VolumeId: "disk-1", VolumeType: "mock", Capacity: "10Gi"},
			wantFields: []string{"external_volume.capacity"},
		},
		{
			name:       "an ID with no driver to reach it through",
			volume:     &ateapipb.ExternalVolume{VolumeId: "disk-1"},
			wantFields: []string{"external_volume.volume_type"},
		},
		{
			name:       "a driver alongside the class that supplies one",
			volume:     &ateapipb.ExternalVolume{StorageClassName: "standard", VolumeType: "mock", Capacity: "10Gi"},
			wantFields: []string{"external_volume.volume_type"},
		},
		{
			name:       "driver metadata for a volume that does not exist yet",
			volume:     &ateapipb.ExternalVolume{StorageClassName: "standard", Capacity: "10Gi", VolumeContext: map[string]string{"fsType": "ext4"}},
			wantFields: []string{"external_volume.volume_context"},
		},
		{
			name:       "provisioning without a size",
			volume:     &ateapipb.ExternalVolume{StorageClassName: "standard"},
			wantFields: []string{"external_volume.capacity"},
		},
		{
			name:       "read write many is not supported yet",
			volume:     &ateapipb.ExternalVolume{StorageClassName: "standard", Capacity: "10Gi", AccessMode: ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY},
			wantFields: []string{"external_volume.access_mode"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &ateapipb.CreateExternalVolumeRequest{ExternalVolume: tc.volume}
			errs := ValidateCustom_CreateExternalVolumeRequest(context.Background(), operation.Operation{Type: operation.Create}, nil, req, nil)

			var gotFields []string
			for _, err := range errs {
				gotFields = append(gotFields, err.Field)
			}
			if diff := cmp.Diff(tc.wantFields, gotFields); diff != "" {
				t.Errorf("offending fields mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateCustomExternalVolumeCapacity(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{value: ""},
		{value: "10Gi"},
		{value: "1500m"},
		{value: "ten gigs", wantErr: true},
		{value: "10GiB", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			value := tc.value
			errs := ValidateCustom_ExternalVolume_Capacity(context.Background(), operation.Operation{Type: operation.Create}, field.NewPath("capacity"), &value, nil)
			if gotErr := len(errs) > 0; gotErr != tc.wantErr {
				t.Errorf("ValidateCustom_ExternalVolume_Capacity(%q) errors = %v, wantErr %v", tc.value, errs, tc.wantErr)
			}
		})
	}
}
