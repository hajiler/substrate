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
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type externalVolumeRefStore interface {
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error)
}

type externalVolumeDeleter interface {
	DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error)
}

// resolveExternalVolumeRefs checks the actor's bindings against the template
// and returns a mount for every ExternalVolume the actor references.
func resolveExternalVolumeRefs(ctx context.Context, st externalVolumeRefStore, actor *ateapipb.Actor, template *ateapipb.ActorTemplate) ([]*ateapipb.ActorVolumeStatus, error) {
	bindings := actor.GetExternalVolumeBindings()
	unnamed := make(map[string]bool)
	for _, vol := range template.GetVolumes() {
		if vol.GetExternalVolumeRef() != nil && vol.GetExternalVolumeRef().GetName() == "" {
			unnamed[vol.GetName()] = true
		}
	}
	for slot := range bindings {
		if !unnamed[slot] {
			return nil, status.Errorf(codes.InvalidArgument, "external_volume_bindings[%q] does not name an unnamed external_volume_ref in the template", slot)
		}
	}

	var mounts []*ateapipb.ActorVolumeStatus
	for _, vol := range template.GetVolumes() {
		ref := vol.GetExternalVolumeRef()
		if ref == nil {
			continue
		}
		name := ref.GetName()
		if name == "" {
			if name = bindings[vol.GetName()]; name == "" {
				return nil, status.Errorf(codes.InvalidArgument, "external_volume_bindings must bind volume %q", vol.GetName())
			}
		}
		volume, err := getReadyExternalVolume(ctx, st, resources.ExternalVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: name})
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, externalVolumeMount(vol.GetName(), volume))
	}
	return mounts, nil
}

func getReadyExternalVolume(ctx context.Context, st externalVolumeRefStore, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error) {
	volume, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume %s: %w", volumeRef, err)
	}
	if state := volume.GetStatus().GetState(); state != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
		return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s is not ready (state %s)", volumeRef, state)
	}
	return volume, nil
}

func externalVolumeMount(templateVolumeName string, volume *ateapipb.ExternalVolume) *ateapipb.ActorVolumeStatus {
	return &ateapipb.ActorVolumeStatus{
		VolumeName:         templateVolumeName,
		ExternalVolumeName: volume.GetMetadata().GetName(),
		StorageVolumeId:    volume.GetVolumeId(),
		VolumeType:         volume.GetVolumeType(),
		VolumeContext:      volume.GetVolumeContext(),
		Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
	}
}

func borrowedVolumeRefs(actor *ateapipb.Actor) []resources.ExternalVolumeRef {
	var refs []resources.ExternalVolumeRef
	for _, vol := range actor.GetStatus().GetActorVolumes() {
		if vol.GetExternalVolumeName() != "" {
			refs = append(refs, resources.ExternalVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: vol.GetExternalVolumeName()})
		}
	}
	return refs
}

func actorRefOf(actor *ateapipb.Actor, active bool) *ateapipb.ActorRef {
	return &ateapipb.ActorRef{
		ActorUid:  actor.GetMetadata().GetUid(),
		ActorName: actor.GetMetadata().GetName(),
		Active:    active,
	}
}

func findActorRef(volume *ateapipb.ExternalVolume, actorUID string) *ateapipb.ActorRef {
	for _, ref := range volume.GetStatus().GetRefs() {
		if ref.GetActorUid() == actorUID {
			return ref
		}
	}
	return nil
}

func setActorRef(volume *ateapipb.ExternalVolume, ref *ateapipb.ActorRef) {
	if existing := findActorRef(volume, ref.GetActorUid()); existing != nil {
		existing.ActorName, existing.Active = ref.GetActorName(), ref.GetActive()
		return
	}
	if volume.Status == nil {
		volume.Status = &ateapipb.ExternalVolumeStatus{}
	}
	volume.Status.Refs = append(volume.Status.Refs, ref)
}

// updateExternalVolumeRefs applies mutate under the volume's version guard.
// TODO: move references to their own table so claims do not contend on one row.
func updateExternalVolumeRefs(ctx context.Context, st externalVolumeRefStore, volumeRef resources.ExternalVolumeRef, mutate func(*ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error) {
	volume, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		return nil, err
	}
	return st.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(volume), mutate)
}

// bindExternalVolumes records an inactive reference for every ExternalVolume
// the actor mounts.
func bindExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapipb.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		_, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.ExternalVolume) error {
			if findActorRef(toUpdate, actor.GetMetadata().GetUid()) == nil {
				setActorRef(toUpdate, actorRefOf(actor, false))
			}
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("while binding external volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}

var errExternalVolumeHeld = errors.New("external volume is held by another actor")

// claimExternalVolumes marks the actor's references active and refreshes the
// recorded mounts. A READ_WRITE_ONCE volume another actor holds is refused.
// TODO: let READ_WRITE_MANY volumes carry several active references.
func claimExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapipb.Actor) ([]*ateapipb.ActorVolumeStatus, error) {
	volumes := slices.Clone(actor.GetStatus().GetActorVolumes())
	actorUID := actor.GetMetadata().GetUid()
	for i, mount := range volumes {
		if mount.GetExternalVolumeName() == "" {
			continue
		}
		volumeRef := resources.ExternalVolumeRef{Atespace: actor.GetMetadata().GetAtespace(), Name: mount.GetExternalVolumeName()}
		var holder string
		claimed, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.ExternalVolume) error {
			if toUpdate.GetStatus().GetState() != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
				return fmt.Errorf("external volume is not ready (state %s)", toUpdate.GetStatus().GetState())
			}
			for _, ref := range toUpdate.GetStatus().GetRefs() {
				if ref.GetActive() && ref.GetActorUid() != actorUID {
					holder = ref.GetActorName()
					return errExternalVolumeHeld
				}
			}
			setActorRef(toUpdate, actorRefOf(actor, true))
			return nil
		})
		switch {
		case err == nil:
		case errors.Is(err, errExternalVolumeHeld):
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s is already held by actor %q; pause or suspend it before resuming another actor", volumeRef, holder)
		case errors.Is(err, store.ErrNotFound):
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s not found", volumeRef)
		case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrUIDConflict):
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		default:
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s: %v", volumeRef, err)
		}
		volumes[i] = externalVolumeMount(mount.GetVolumeName(), claimed)
	}
	return volumes, nil
}

// deactivateExternalVolumes marks the actor's references inactive.
func deactivateExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapipb.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		_, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.ExternalVolume) error {
			if ref := findActorRef(toUpdate, actor.GetMetadata().GetUid()); ref != nil {
				ref.Active = false
			}
			return nil
		})
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, fmt.Errorf("while deactivating external volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}

// releaseExternalVolumes drops the actor's references and deletes each volume
// left without references whose delete trigger is LAST_ACTOR.
func releaseExternalVolumes(ctx context.Context, st externalVolumeRefStore, deleter externalVolumeDeleter, actor *ateapipb.Actor) error {
	var errs []error
	for _, volumeRef := range borrowedVolumeRefs(actor) {
		released, err := updateExternalVolumeRefs(ctx, st, volumeRef, func(toUpdate *ateapipb.ExternalVolume) error {
			toUpdate.Status.Refs = slices.DeleteFunc(toUpdate.GetStatus().GetRefs(), func(ref *ateapipb.ActorRef) bool {
				return ref.GetActorUid() == actor.GetMetadata().GetUid()
			})
			return nil
		})
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("while releasing external volume %s: %w", volumeRef, err))
			continue
		}
		if released.GetDeleteTrigger() != ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR || len(released.GetStatus().GetRefs()) > 0 {
			continue
		}
		_, err = deleter.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{UID: released.GetMetadata().GetUid()})
		if code := status.Code(err); err != nil && code != codes.NotFound && code != codes.FailedPrecondition {
			errs = append(errs, fmt.Errorf("while deleting external volume %s: %w", volumeRef, err))
		}
	}
	return errors.Join(errs...)
}
