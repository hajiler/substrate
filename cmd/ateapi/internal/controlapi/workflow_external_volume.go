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
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// ExternalVolumeWorkflow handles the multi-step create and delete of an
// ExternalVolume, each of which spans a store write and a CSI call.
type ExternalVolumeWorkflow struct {
	store              externalVolumeWorkflowStore
	storageClassLister storagev1listers.StorageClassLister
	pluginRegistry     VolumePluginRegistry
}

// NewExternalVolumeWorkflow creates a new ExternalVolumeWorkflow.
func NewExternalVolumeWorkflow(
	store externalVolumeWorkflowStore,
	storageClassLister storagev1listers.StorageClassLister,
	pluginRegistry VolumePluginRegistry,
) *ExternalVolumeWorkflow {
	return &ExternalVolumeWorkflow{
		store:              store,
		storageClassLister: storageClassLister,
		pluginRegistry:     pluginRegistry,
	}
}

type externalVolumeWorkflowStore interface {
	CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error)
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error)
	DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// CreateExternalVolume reserves the name, provisions the volume under an ID
// derived from the record's UID, and stamps the handle the driver returned.
// A create that dies leaves a PENDING record; delete it to retry.
func (w *ExternalVolumeWorkflow) CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error) {
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)

	ctx, lease, err := acquireExternalVolumeLease(ctx, w.store, volumeRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	toCreate, provisioning, err := w.resolveExternalVolumeSource(ctx, volume)
	if err != nil {
		return nil, err
	}
	reserved, err := w.ensureExternalVolumeReserved(ctx, volumeRef, toCreate)
	if err != nil {
		return nil, err
	}
	if provisioning == nil {
		return reserved, nil
	}
	volumeID, volumeContext, err := w.ensureExternalVolumeProvisioned(ctx, reserved, provisioning)
	if err != nil {
		return nil, err
	}
	return w.ensureExternalVolumeFinalized(ctx, reserved, volumeID, volumeContext)
}

type externalVolumeProvisioning struct {
	provisioner string
	parameters  map[string]string
}

// resolveExternalVolumeSource returns the record to reserve and, unless the
// request registers an existing volume, what provisioning it needs.
func (w *ExternalVolumeWorkflow) resolveExternalVolumeSource(ctx context.Context, volume *ateapipb.ExternalVolume) (_ *ateapipb.ExternalVolume, _ *externalVolumeProvisioning, err error) {
	_, done := stepSpan(ctx, "ResolveExternalVolumeSource")
	defer func() { err = done(err) }()

	toCreate := &ateapipb.ExternalVolume{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: volume.GetMetadata().GetAtespace(),
			Name:     volume.GetMetadata().GetName(),
		},
		DeleteTrigger:    volume.GetDeleteTrigger(),
		AccessMode:       volume.GetAccessMode(),
		StorageClassName: volume.GetStorageClassName(),
		VolumeId:         volume.GetVolumeId(),
		VolumeType:       volume.GetVolumeType(),
		VolumeContext:    volume.GetVolumeContext(),
		Capacity:         volume.GetCapacity(),
		Status:           &ateapipb.ExternalVolumeStatus{State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_PENDING},
	}

	var sc *storagev1.StorageClass
	if scName := volume.GetStorageClassName(); scName != "" {
		var err error
		if sc, err = w.storageClassLister.Get(scName); err != nil {
			if k8serrors.IsNotFound(err) {
				return nil, nil, status.Errorf(codes.FailedPrecondition, "StorageClass %q not found", scName)
			}
			return nil, nil, status.Errorf(codes.Internal, "failed to get StorageClass %q: %v", scName, err)
		}
	}

	if toCreate.VolumeId != "" {
		if toCreate.DeleteTrigger == ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED {
			toCreate.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL
		}
		if toCreate.VolumeType == "" && sc != nil {
			toCreate.VolumeType = sc.Provisioner
		}
		toCreate.Status.State = ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY
		return toCreate, nil, nil
	}
	if sc == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "one of storage_class_name or volume_id is required")
	}
	if toCreate.DeleteTrigger == ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED {
		toCreate.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR
	}
	toCreate.VolumeType = sc.Provisioner
	return toCreate, &externalVolumeProvisioning{provisioner: sc.Provisioner, parameters: sc.Parameters}, nil
}

func (w *ExternalVolumeWorkflow) ensureExternalVolumeReserved(ctx context.Context, volumeRef resources.ExternalVolumeRef, toCreate *ateapipb.ExternalVolume) (_ *ateapipb.ExternalVolume, err error) {
	ctx, done := stepSpan(ctx, "ReserveExternalVolume")
	defer func() { err = done(err) }()

	stored, err := w.store.CreateExternalVolume(ctx, toCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", volumeRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Errorf(codes.AlreadyExists, "ExternalVolume %s already exists; delete it and create it again to retry", volumeRef)
	}
	return nil, fmt.Errorf("while reserving external volume %s: %w", volumeRef, err)
}

// TODO: pass access_mode to CreateVolume as the volume capability once
// READ_WRITE_MANY is accepted.
func (w *ExternalVolumeWorkflow) ensureExternalVolumeProvisioned(ctx context.Context, volume *ateapipb.ExternalVolume, provisioning *externalVolumeProvisioning) (_ string, _ map[string]string, err error) {
	ctx, done := stepSpan(ctx, "ProvisionExternalVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	plugin, err := w.pluginRegistry.GetPlugin(ctx, provisioning.provisioner)
	if err != nil {
		return "", nil, status.Errorf(codes.FailedPrecondition, "failed to get volume plugin for driver %q (StorageClass %q): %v", provisioning.provisioner, volume.GetStorageClassName(), err)
	}
	volumeID, volumeContext, err := plugin.CreateVolume(ctx, externalVolumeID(volume.GetMetadata().GetUid()), volume.GetCapacity(), provisioning.provisioner, provisioning.parameters)
	if err != nil {
		return "", nil, status.Errorf(codes.Internal, "failed to create external volume %s: %v", volumeRef, err)
	}
	return volumeID, volumeContext, nil
}

func (w *ExternalVolumeWorkflow) ensureExternalVolumeFinalized(ctx context.Context, volume *ateapipb.ExternalVolume, volumeID string, volumeContext map[string]string) (_ *ateapipb.ExternalVolume, err error) {
	ctx, done := stepSpan(ctx, "FinalizeExternalVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	stored, err := w.store.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(volume), func(toUpdate *ateapipb.ExternalVolume) error {
		toUpdate.VolumeId = volumeID
		toUpdate.VolumeContext = volumeContext
		toUpdate.Status.State = ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ExternalVolume %s was deleted while it was being created, please retry", volumeRef)
		}
		return nil, fmt.Errorf("while finalizing external volume %s: %w", volumeRef, err)
	}
	return stored, nil
}

// DeleteExternalVolume deletes the storage and then removes the record, so a
// failure part way is retried by deleting again. Rejected while any actor
// references the volume.
func (w *ExternalVolumeWorkflow) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error) {
	ctx, lease, err := acquireExternalVolumeLease(ctx, w.store, volumeRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	stored, err := w.store.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume: %w", err)
	}
	if err := precondition.Check(stored.GetMetadata()); err != nil {
		return nil, externalVolumeDeleteConflict(err, volumeRef, precondition)
	}
	if refs := stored.GetStatus().GetRefs(); len(refs) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s is still referenced by %d actor(s), starting with %q", volumeRef, len(refs), refs[0].GetActorName())
	}
	if err := w.reclaimExternalVolume(ctx, stored); err != nil {
		return nil, err
	}

	volume, err := w.store.DeleteExternalVolume(ctx, volumeRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ExternalVolume %s not found", volumeRef)
		}
		if errors.Is(err, store.ErrUIDConflict) || errors.Is(err, store.ErrVersionConflict) {
			return nil, externalVolumeDeleteConflict(err, volumeRef, precondition)
		}
		return nil, fmt.Errorf("while deleting external volume: %w", err)
	}
	return volume, nil
}

func externalVolumeDeleteConflict(err error, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) error {
	if errors.Is(err, store.ErrUIDConflict) {
		return status.Errorf(codes.Aborted, "ExternalVolume %s does not have uid %s", volumeRef, precondition.UID)
	}
	return status.Error(codes.Aborted, "concurrent update conflict, please retry")
}

// TODO: add a reclaim policy that leaves the storage in place.
func (w *ExternalVolumeWorkflow) reclaimExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (err error) {
	ctx, done := stepSpan(ctx, "ReclaimExternalVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	if volume.GetVolumeType() == "" {
		markSkipped(ctx, "volume names no driver")
		return nil
	}
	volumeID := volume.GetVolumeId()
	if volumeID == "" {
		volumeID = externalVolumeID(volume.GetMetadata().GetUid())
	}
	plugin, err := w.pluginRegistry.GetPlugin(ctx, volume.GetVolumeType())
	if err != nil {
		return fmt.Errorf("while getting the volume plugin for %q of external volume %s: %w", volume.GetVolumeType(), volumeRef, err)
	}
	if err := plugin.DeleteVolume(ctx, volumeID); err != nil {
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "Volume not found during delete, assuming already deleted", slog.String("volume_id", volumeID))
			return nil
		}
		return fmt.Errorf("while reclaiming the storage %q of external volume %s: %w", volumeID, volumeRef, err)
	}
	return nil
}

func externalVolumeID(volumeUID string) string {
	return "substrate-" + volumeUID
}
