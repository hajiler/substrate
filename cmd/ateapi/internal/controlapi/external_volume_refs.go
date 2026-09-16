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

// externalVolumeRefStore enumerates the store methods the claim and release of
// an ExternalVolume reference need, and nothing more.
type externalVolumeRefStore interface {
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error)
}

// claimExternalVolumes takes a reference on every ExternalVolume the actor's
// template borrows, and returns the mounts to record on the actor.
//
// A reference is what keeps the volume from being deleted out from under the
// actor. It is held only while the actor is resumed, so this runs on every
// resume rather than once at create, and is idempotent: a reference already
// held is left as it is.
//
// The result replaces the borrowed entries in the actor's volume list. Entries
// for volumes the actor owns are returned untouched.
func claimExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapipb.Actor, template *ateapipb.ActorTemplate) ([]*ateapipb.ActorVolumeStatus, error) {
	volumes := slices.Clone(actor.GetStatus().GetActorVolumes())
	atespace := actor.GetMetadata().GetAtespace()
	actorRef := &ateapipb.ActorRef{
		ActorUid:  actor.GetMetadata().GetUid(),
		ActorName: actor.GetMetadata().GetName(),
	}

	for _, templateVol := range template.GetVolumes() {
		ref := templateVol.GetExternalVolumeRef()
		if ref == nil {
			continue
		}
		volumeRef := resources.ExternalVolumeRef{Atespace: atespace, Name: ref.GetName()}
		volume, err := claimExternalVolume(ctx, st, volumeRef, actorRef)
		if err != nil {
			return nil, err
		}

		mount := &ateapipb.ActorVolumeStatus{
			VolumeName:         templateVol.GetName(),
			ExternalVolumeName: volume.GetMetadata().GetName(),
			StorageVolumeId:    volume.GetVolumeId(),
			VolumeType:         volume.GetVolumeType(),
			VolumeContext:      volume.GetVolumeContext(),
			Status:             ateapipb.ActorVolumeStatus_STATUS_CREATED,
		}
		if idx := slices.IndexFunc(volumes, func(v *ateapipb.ActorVolumeStatus) bool {
			return v.GetVolumeName() == mount.GetVolumeName()
		}); idx >= 0 {
			volumes[idx] = mount
		} else {
			volumes = append(volumes, mount)
		}
	}
	return volumes, nil
}

// claimExternalVolume adds actorRef to one ExternalVolume's reference set and
// returns the volume, whose handle the caller records on the actor.
func claimExternalVolume(ctx context.Context, st externalVolumeRefStore, volumeRef resources.ExternalVolumeRef, actorRef *ateapipb.ActorRef) (*ateapipb.ExternalVolume, error) {
	volume, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume %s: %w", volumeRef, err)
	}
	// A pending volume names no disk yet, so there is nothing to mount.
	if state := volume.GetStatus().GetState(); state != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
		return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s is not ready (state %s)", volumeRef, state)
	}
	if hasActorRef(volume, actorRef.GetActorUid()) {
		return volume, nil
	}

	claimed, err := st.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(volume), func(toUpdate *ateapipb.ExternalVolume) error {
		if hasActorRef(toUpdate, actorRef.GetActorUid()) {
			return nil
		}
		toUpdate.Status.Refs = append(toUpdate.Status.Refs, actorRef)
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while claiming external volume %s: %w", volumeRef, err)
	}
	return claimed, nil
}

// releaseExternalVolumes drops the actor's reference on every ExternalVolume it
// borrows, so that the volume becomes deletable again once nothing holds it.
//
// Driven off the actor's own volume list rather than its template, because the
// template can be deleted while the actor is still being torn down. Idempotent,
// and a volume that is already gone is not an error: there is then nothing left
// to hold a reference on.
func releaseExternalVolumes(ctx context.Context, st externalVolumeRefStore, actor *ateapipb.Actor) error {
	atespace, actorUID := actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid()

	var errs []error
	for _, vol := range actor.GetStatus().GetActorVolumes() {
		if vol.GetExternalVolumeName() == "" {
			continue
		}
		volumeRef := resources.ExternalVolumeRef{Atespace: atespace, Name: vol.GetExternalVolumeName()}
		if err := releaseExternalVolume(ctx, st, volumeRef, actorUID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func releaseExternalVolume(ctx context.Context, st externalVolumeRefStore, volumeRef resources.ExternalVolumeRef, actorUID string) error {
	volume, err := st.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while getting external volume %s: %w", volumeRef, err)
	}
	if !hasActorRef(volume, actorUID) {
		return nil
	}

	_, err = st.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(volume), func(toUpdate *ateapipb.ExternalVolume) error {
		toUpdate.Status.Refs = slices.DeleteFunc(toUpdate.GetStatus().GetRefs(), func(ref *ateapipb.ActorRef) bool {
			return ref.GetActorUid() == actorUID
		})
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil
		}
		return fmt.Errorf("while releasing external volume %s: %w", volumeRef, err)
	}
	return nil
}

func hasActorRef(volume *ateapipb.ExternalVolume, actorUID string) bool {
	return slices.ContainsFunc(volume.GetStatus().GetRefs(), func(ref *ateapipb.ActorRef) bool {
		return ref.GetActorUid() == actorUID
	})
}
