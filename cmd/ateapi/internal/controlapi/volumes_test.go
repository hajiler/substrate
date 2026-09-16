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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

type fakeStorageClassLister struct {
	storageClasses map[string]*storagev1.StorageClass
}

func (f *fakeStorageClassLister) List(selector k8slabels.Selector) (ret []*storagev1.StorageClass, err error) {
	return nil, nil
}

func (f *fakeStorageClassLister) Get(name string) (*storagev1.StorageClass, error) {
	sc, ok := f.storageClasses[name]
	if !ok {
		return nil, k8serrors.NewNotFound(storagev1.Resource("storageclass"), name)
	}
	return sc, nil
}

var _ storagev1listers.StorageClassLister = (*fakeStorageClassLister)(nil)

func TestInitialActorVolumes_PendingState(t *testing.T) {
	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "data-vol-1",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
			{
				Name: "scratch-vol",
			},
			{
				Name:       "durable-vol",
				DurableDir: &ateapipb.DurableDirVolumeSource{},
			},
			{
				Name: "data-vol-2",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "fast",
				},
			},
		},
	}

	want := []*ateapipb.ActorVolumeStatus{
		{
			VolumeName: "data-vol-1",
			VolumeType: "mock-standard",
			Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
		},
		{
			VolumeName: "data-vol-2",
			VolumeType: "mock-fast",
			Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
		},
	}

	scLister := &fakeStorageClassLister{
		storageClasses: map[string]*storagev1.StorageClass{
			"standard": {
				ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
				Provisioner: "mock-standard",
			},
			"fast": {
				ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
				Provisioner: "mock-fast",
			},
		},
	}
	initVols, err := initialActorVolumes(context.Background(), scLister, tmpl)
	if err != nil {
		t.Fatalf("initialActorVolumes failed: %v", err)
	}
	if diff := cmp.Diff(want, initVols, protocmp.Transform()); diff != "" {
		t.Errorf("initialActorVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActorVolumes(t *testing.T) {
	ctx := context.Background()

	standardTmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "data-vol",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
		},
	}

	multiVolTmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "vol1",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
			{
				Name: "vol2",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
			{
				Name: "vol3",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "standard",
				},
			},
		},
	}

	tests := []struct {
		name         string
		tmpl         *ateapipb.ActorTemplate
		inputVolumes []*ateapipb.ActorVolumeStatus
		wantErr      bool
		wantRes      []*ateapipb.ActorVolumeStatus
	}{
		{
			name: "partial failure returns error and preserves succeeded, failed, and remaining volumes",
			tmpl: multiVolTmpl,
			inputVolumes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName: "vol1",
					VolumeType: "mock-standard",
					Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
				},
				{
					VolumeName: "vol2",
					Status:     ateapipb.ActorVolumeStatus_STATUS_DELETING,
				},
				{
					VolumeName: "vol3",
					Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName:      "vol1",
					StorageVolumeId: "mock-vol-substrate-actor-uid-123-vol1",
					VolumeType:      "mock-standard",
					Status:          ateapipb.ActorVolumeStatus_STATUS_CREATED,
				},
				{
					VolumeName: "vol2",
					Status:     ateapipb.ActorVolumeStatus_STATUS_DELETING,
				},
				{
					VolumeName: "vol3",
					Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
				},
			},
		},
		{
			name: "created volume status succeeds",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName:      "data-vol",
					StorageVolumeId: "existing-vol-id",
					Status:          ateapipb.ActorVolumeStatus_STATUS_CREATED,
				},
			},
			wantErr: false,
			wantRes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName:      "data-vol",
					StorageVolumeId: "existing-vol-id",
					Status:          ateapipb.ActorVolumeStatus_STATUS_CREATED,
				},
			},
		},
		{
			name: "unspecified volume status returns error",
			tmpl: standardTmpl,
			inputVolumes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName: "data-vol",
					Status:     ateapipb.ActorVolumeStatus_STATUS_UNSPECIFIED,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName: "data-vol",
					Status:     ateapipb.ActorVolumeStatus_STATUS_UNSPECIFIED,
				},
			},
		},
		{
			name: "volume not found in template returns error",
			tmpl: &ateapipb.ActorTemplate{},
			inputVolumes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName: "missing-vol",
					Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
				},
			},
			wantErr: true,
			wantRes: []*ateapipb.ActorVolumeStatus{
				{
					VolumeName: "missing-vol",
					Status:     ateapipb.ActorVolumeStatus_STATUS_PENDING,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := volume.NewMockVolumePlugin()
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock-standard": plugin,
					"mock-fast":     plugin,
				},
			}
			scLister := &fakeStorageClassLister{
				storageClasses: map[string]*storagev1.StorageClass{
					"standard": {
						ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
						Provisioner: "mock-standard",
					},
					"fast": {
						ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
						Provisioner: "mock-fast",
					},
				},
			}
			res, err := createActorVolumes(ctx, registry, scLister, "actor-uid-123", tt.tmpl, tt.inputVolumes)
			if (err != nil) != tt.wantErr {
				t.Errorf("createActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantRes, res, protocmp.Transform()); diff != "" {
				t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestBorrowedVolumesAreNotProvisionedOrDeleted covers the whole life of a
// mount borrowed from an ExternalVolume: the claim on resume is what fills it
// in, so neither end of the actor's own volume lifecycle may touch the disk.
func TestBorrowedVolumesAreNotProvisionedOrDeleted(t *testing.T) {
	ctx := context.Background()

	template := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{Name: "shared-data", ExternalVolumeRef: &ateapipb.ExternalVolumeRef{Name: "shared"}},
		},
	}
	scLister := &fakeStorageClassLister{storageClasses: map[string]*storagev1.StorageClass{}}

	// A borrowed volume names no driver or handle until the claim resolves it,
	// so the actor starts with nothing recorded for it.
	initial, err := initialActorVolumes(ctx, scLister, template)
	if err != nil {
		t.Fatalf("initialActorVolumes: %v", err)
	}
	if len(initial) != 0 {
		t.Errorf("initialActorVolumes() = %v, want no entry for a borrowed volume", initial)
	}

	claimed := []*ateapipb.ActorVolumeStatus{{
		VolumeName:         "shared-data",
		ExternalVolumeName: "shared",
		StorageVolumeId:    "disk/shared",
		VolumeType:         "mock",
		Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
	}}
	plugin := &trackingVolumePlugin{}
	registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}}

	got, err := createActorVolumes(ctx, registry, scLister, "actor-uid-123", template, claimed)
	if err != nil {
		t.Fatalf("createActorVolumes: %v", err)
	}
	if diff := cmp.Diff(claimed, got, protocmp.Transform()); diff != "" {
		t.Errorf("createActorVolumes() mismatch (-want +got):\n%s", diff)
	}

	if err := deleteActorVolumes(ctx, registry, "actor-uid-123", claimed); err != nil {
		t.Fatalf("deleteActorVolumes: %v", err)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Errorf("deleted %v, want the borrowed disk left to its ExternalVolume", plugin.deletedIDs)
	}
}

type trackingVolumePlugin struct {
	volume.VolumePluginControlPlane
	deletedIDs []string
}

func (t *trackingVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	t.deletedIDs = append(t.deletedIDs, volumeID)
	return nil
}

func TestDeleteActorVolumes(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		actorUID    string
		volumes     []*ateapipb.ActorVolumeStatus
		wantDeleted []string
		wantErr     bool
	}{
		{
			name:     "uses storage volume ID when present",
			actorUID: "uid-abc",
			volumes: []*ateapipb.ActorVolumeStatus{
				{VolumeName: "vol1", StorageVolumeId: "storage-vol-123", VolumeType: "mock"},
			},
			wantDeleted: []string{"storage-vol-123"},
			wantErr:     false,
		},
		{
			name:     "falls back to actorVolumeID when storage volume ID is empty regardless of status",
			actorUID: "uid-abc",
			volumes: []*ateapipb.ActorVolumeStatus{
				{VolumeName: "vol1", StorageVolumeId: "", Status: ateapipb.ActorVolumeStatus_STATUS_CREATED, VolumeType: "mock"},
			},
			wantDeleted: []string{"substrate-uid-abc-vol1"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &trackingVolumePlugin{}
			registry := &mockPluginRegistry{
				plugins: map[string]volume.VolumePluginControlPlane{
					"mock": plugin,
				},
			}
			err := deleteActorVolumes(ctx, registry, tt.actorUID, tt.volumes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("deleteActorVolumes() error = %v, wantErr %v", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.wantDeleted, plugin.deletedIDs); diff != "" {
				t.Errorf("deletedIDs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type mockPluginRegistry struct {
	plugins map[string]volume.VolumePluginControlPlane
}

func (m *mockPluginRegistry) GetPlugin(ctx context.Context, name string) (volume.VolumePluginControlPlane, error) {
	p, ok := m.plugins[name]
	if !ok {
		return nil, fmt.Errorf("plugin %q not found in mock registry", name)
	}
	return p, nil
}
