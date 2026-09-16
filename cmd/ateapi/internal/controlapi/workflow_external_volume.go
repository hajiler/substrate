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
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// ExternalVolumeWorkflow handles the multi-step operations on an
// ExternalVolume: the create that spans a reservation and a CSI call, and the
// delete that spans a CSI call and a row removal.
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

// externalVolumeWorkflowStore enumerates the exact storage methods needed by
// ExternalVolumeWorkflow and nothing more.
type externalVolumeWorkflowStore interface {
	CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error)
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error)
	DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// CreateExternalVolume creates the volume the request describes.
//
// It is built in 3 phases:
//  1. Reserve the name, recording what the volume is to be made of.
//  2. Provision the volume through its CSI driver, under an ID derived from
//     the reserved record's UID.
//  3. Finalize: stamp the handle the driver returned and mark the record ready.
//
// Registering a volume that already exists skips phase 2: the handle is known
// up front, so the reservation is already the finished record.
//
// Not idempotent: the name is taken as soon as phase 1 lands, so a create that
// dies after it leaves a pending volume and every later create under that name
// is AlreadyExists. To retry, delete the volume, which collects whatever the
// failed attempt stranded, and create it again.
func (w *ExternalVolumeWorkflow) CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error) {
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)

	// Serializes against a delete of the volume this creates, which would
	// otherwise collect the disk while it is being provisioned.
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

// externalVolumeProvisioning is what the CSI create call needs, read off the
// StorageClass the volume is being provisioned from.
type externalVolumeProvisioning struct {
	provisioner string
	parameters  map[string]string
}

// resolveExternalVolumeSource reads the volume's StorageClass, if it names one,
// and returns the record to reserve alongside what provisioning it needs.
// A volume that arrives with a handle needs none, and is reserved ready.
func (w *ExternalVolumeWorkflow) resolveExternalVolumeSource(ctx context.Context, volume *ateapipb.ExternalVolume) (_ *ateapipb.ExternalVolume, _ *externalVolumeProvisioning, err error) {
	// The StorageClass comes from an informer cache, so nothing here takes the
	// span's context; it is opened only so the step shows up in the trace.
	_, done := stepSpan(ctx, "ResolveExternalVolumeSource")
	defer func() { err = done(err) }()

	toCreate := &ateapipb.ExternalVolume{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: volume.GetMetadata().GetAtespace(),
			Name:     volume.GetMetadata().GetName(),
		},
		ReclaimPolicy:    volume.GetReclaimPolicy(),
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
	if toCreate.ReclaimPolicy == ateapipb.ReclaimPolicy_RECLAIM_POLICY_UNSPECIFIED {
		toCreate.ReclaimPolicy = defaultReclaimPolicy(sc)
	}

	// A volume that already exists has nothing to provision: the class, if one
	// is named at all, is recorded as documentation of what the disk is.
	if toCreate.VolumeId != "" {
		if toCreate.VolumeType == "" && sc != nil {
			toCreate.VolumeType = sc.Provisioner
		}
		toCreate.Status.State = ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY
		return toCreate, nil, nil
	}
	if sc == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "one of storage_class_name or volume_id is required")
	}
	toCreate.VolumeType = sc.Provisioner
	return toCreate, &externalVolumeProvisioning{provisioner: sc.Provisioner, parameters: sc.Parameters}, nil
}

// defaultReclaimPolicy is what an ExternalVolume reclaims by when the request
// did not say. A provisioned volume follows its StorageClass, which is where a
// cluster already states this intent; a volume Substrate did not create is
// retained, because collecting a disk somebody else made is not ours to do by
// default.
func defaultReclaimPolicy(sc *storagev1.StorageClass) ateapipb.ReclaimPolicy {
	if sc == nil || sc.ReclaimPolicy == nil {
		return ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN
	}
	if *sc.ReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
		return ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE
	}
	return ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN
}

// ensureExternalVolumeReserved takes the volume's name.
//
// A name already taken is AlreadyExists, whether the volume holding it is
// finished or was left pending by a create that died. Resuming a pending row
// would mean deciding whether the disk the failed attempt may have made is
// still the one being asked for; deleting the volume collects it and frees the
// name, so a retry is a delete followed by a create.
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

// ensureExternalVolumeProvisioned asks the CSI driver for the disk, under an ID
// derived from the reserved record's freshly minted UID. The UID rather than
// (atespace, name) because both of those are DNS labels that may contain
// hyphens, so the concatenation would not be unambiguously decodable.
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

// ensureExternalVolumeFinalized stamps the handle the driver returned. Until
// this lands the volume is pending and cannot be referenced; deleting it
// collects whatever disk was made.
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

// DeleteExternalVolume reclaims the disk, if the volume's policy says to, and
// then removes the row, in that order: the row is the only handle on that
// disk, so dropping it first would leak. A failure at any point fails the whole
// RPC; the client retries the same delete, which rediscovers the work from the
// row and resumes over whatever is left.
//
// Rejected while any actor still holds the volume. A pending volume is
// deletable, which is how a create that died is cleaned up.
func (w *ExternalVolumeWorkflow) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error) {
	// Serializes against a create of the same volume, which would otherwise
	// still be provisioning the disk this is collecting.
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
	if refs := stored.GetStatus().GetRefs(); len(refs) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s is still referenced by %d actor(s), starting with %q", volumeRef, len(refs), refs[0].GetActorName())
	}
	if err := w.reclaimExternalVolume(ctx, stored); err != nil {
		return nil, err
	}

	volume, err := w.store.DeleteExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while deleting external volume: %w", err)
	}
	return volume, nil
}

// reclaimExternalVolume deletes the disk the volume names, if its reclaim
// policy asks for that. It tolerates a disk that is already gone, so a retry
// finishes cleanly.
func (w *ExternalVolumeWorkflow) reclaimExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (err error) {
	ctx, done := stepSpan(ctx, "ReclaimExternalVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	if volume.GetReclaimPolicy() != ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE {
		markSkipped(ctx, "reclaim policy retains the volume")
		return nil
	}
	// A create that died before the driver answered leaves no handle to
	// collect by. Fall back to the ID the create would have asked for, which
	// is derived from the record's UID and so cannot name anyone else's disk.
	volumeID := volume.GetVolumeId()
	if volumeID == "" {
		volumeID = externalVolumeID(volume.GetMetadata().GetUid())
	}
	if volume.GetVolumeType() == "" {
		// Nothing was ever resolved far enough to name a driver, so there is
		// nothing that could have been provisioned either.
		markSkipped(ctx, "volume names no driver")
		return nil
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
		return fmt.Errorf("while reclaiming the disk %q of external volume %s: %w", volumeID, volumeRef, err)
	}
	return nil
}

// externalVolumeID is the storage-level ID of the disk an ExternalVolume owns.
func externalVolumeID(volumeUID string) string {
	return "substrate-" + volumeUID
}
