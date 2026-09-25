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

// sharedVolumePlugin records every storage operation the control plane makes.
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

func setupExternalVolumeTest(t *testing.T, ns string) (*testContext, *sharedVolumePlugin) {
	t.Helper()
	plugin := &sharedVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	return tc, plugin
}

func createExternalVolume(t *testing.T, tc *testContext, name string, trigger ateapipb.DeleteTrigger) *ateapipb.ExternalVolume {
	t.Helper()
	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			StorageClassName: "standard",
			Capacity:         "10Gi",
			DeleteTrigger:    trigger,
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

func getExternalVolume(t *testing.T, tc *testContext, name string) *ateapipb.ExternalVolume {
	t.Helper()
	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapipb.GetExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetExternalVolume(%s) failed: %v", name, err)
	}
	return stored
}

func TestCreateExternalVolume_Provisioned(t *testing.T) {
	ns := namespaceForTest("ns-extvol-provisioned")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	wantDiskName := "substrate-" + created.GetMetadata().GetUid()
	gotCreated, _, _, _ := plugin.snapshot()
	if diff := cmp.Diff([]string{wantDiskName}, gotCreated); diff != "" {
		t.Errorf("provisioned disks mismatch (-want +got):\n%s", diff)
	}
	if got, want := created.GetVolumeId(), "disk/"+wantDiskName; got != want {
		t.Errorf("volume_id = %q, want %q", got, want)
	}
	if got, want := created.GetVolumeType(), "substrate.io/mock"; got != want {
		t.Errorf("volume_type = %q, want %q", got, want)
	}
	if got := created.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR {
		t.Errorf("delete_trigger = %v, want LAST_ACTOR", got)
	}
	if got := created.GetAccessMode(); got != ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE {
		t.Errorf("access_mode = %v, want READ_WRITE_ONCE", got)
	}

	if diff := cmp.Diff(created, getExternalVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
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
		t.Errorf("state = %v, want READY", got)
	}
	if got := created.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}
	if gotCreated, _, _, _ := plugin.snapshot(); len(gotCreated) != 0 {
		t.Errorf("provisioned %v, want nothing provisioned for a registered volume", gotCreated)
	}
}

func TestCreateExternalVolume_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-extvol-rejected")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	first := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "shared"},
			StorageClassName: "fast",
			Capacity:         "20Gi",
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists,
		fmt.Sprintf("ExternalVolume %s/shared already exists; delete it and create it again to retry", testAtespace))
	if diff := cmp.Diff(first, getExternalVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "many"},
			StorageClassName: "standard",
			Capacity:         "10Gi",
			AccessMode:       ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY,
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "access_mode")
}

func TestUpdateExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-update")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	toUpdate := func(in *ateapipb.ExternalVolume) *ateapipb.ExternalVolume {
		return &ateapipb.ExternalVolume{
			Metadata:         in.GetMetadata(),
			DeleteTrigger:    in.GetDeleteTrigger(),
			AccessMode:       in.GetAccessMode(),
			StorageClassName: in.GetStorageClassName(),
			Capacity:         in.GetCapacity(),
			VolumeId:         in.GetVolumeId(),
			VolumeType:       in.GetVolumeType(),
			VolumeContext:    in.GetVolumeContext(),
		}
	}

	req := toUpdate(created)
	req.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL
	updated, err := tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	if err != nil {
		t.Fatalf("UpdateExternalVolume failed: %v", err)
	}
	if got := updated.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	req = toUpdate(updated)
	req.VolumeId = "disk/somewhere-else"
	_, err = tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")

	req = toUpdate(updated)
	req.AccessMode = ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY
	_, err = tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "access_mode")
}

func TestDeleteExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-delete")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"}

	_, err := tc.client.DeleteExternalVolume(context.Background(), &ateapipb.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapipb.DeleteOptions{Version: created.GetMetadata().GetVersion() + 1},
	})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	if _, err := tc.client.DeleteExternalVolume(context.Background(), &ateapipb.DeleteExternalVolumeRequest{ExternalVolume: ref}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
	if _, deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{created.GetVolumeId()}, deleted) {
		t.Errorf("deleted disks = %v, want %v", deleted, []string{created.GetVolumeId()})
	}
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapipb.DeleteExternalVolumeRequest{ExternalVolume: ref})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/shared not found", testAtespace))
}
