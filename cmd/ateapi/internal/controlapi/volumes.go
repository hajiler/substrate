// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"log/slog"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO: we should persist creation first so that we can handle background cleanup.
// this probably requires us to add a PROVISIONING actor state.

// createActorVolumes provisions external volumes specified in the actor template.
// It returns the list of created external volumes, or an error if any creation fails.
// If any volume creation fails, it cleans up any volumes created in this call on a best-effort basis.
func (s *Service) createActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, template *atev1alpha1.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.Spec.Volumes {
		if vol.ExternalVolumeTemplate != nil {
			// Use a unique name for the volume to ensure idempotency
			uniqueVolName := actorVolumeID(ref, vol.Name)

			scName := vol.ExternalVolumeTemplate.StorageClassName
			sc, err := s.kubeClient.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get StorageClass %q: %v", scName, err)
			}
			driverName := sc.Provisioner

			plugin, err := s.GetPlugin(ctx, driverName)
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "failed to get volume plugin for driver %q (StorageClass %q): %v", driverName, scName, err)
			}

			storageVolumeID, volCtx, err := plugin.CreateVolume(ctx, uniqueVolName, vol.ExternalVolumeTemplate.Capacity.String(), scName, sc.Parameters)
			if err != nil {
				// TODO: need better system - best effort cleanup of already created volumes
				s.deleteActorVolumes(ctx, ref, volumes)
				return nil, status.Errorf(codes.Internal, "failed to create volume %q: %v", vol.Name, err)
			}
			volumes = append(volumes, &ateapipb.ExternalVolume{
				ActorVolumeId:   uniqueVolName,
				StorageVolumeId: storageVolumeID,
				VolumeType:      driverName,
				Status:          ateapipb.ExternalVolume_CREATED,
				VolumeContext:   volCtx,
			})
		}
	}
	return volumes, nil
}

// deleteActorVolumes deletes all external volumes in the list on a best-effort basis.
func (s *Service) deleteActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume) {
	for _, vol := range volumes {
		plugin, err := s.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			slog.ErrorContext(ctx, "failed to get volume plugin during cleanup", slog.String("volume_type", vol.GetVolumeType()), slog.String("volume_id", vol.GetStorageVolumeId()), slog.Any("error", err))
			continue
		}
		if err := plugin.DeleteVolume(ctx, vol.GetStorageVolumeId()); err != nil {
			slog.ErrorContext(ctx, "failed to delete volume",
				slog.String("atespace", ref.GetAtespace()),
				slog.String("actor_id", ref.GetName()),
				slog.String("volume_id", vol.GetStorageVolumeId()),
				slog.Any("error", err))
		}
	}
}

// getMountedActorVolumes filters the actor's volumes and returns only those that are declared and mounted in the ActorTemplate.
func getMountedActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume, template *atev1alpha1.ActorTemplate) []*ateapipb.ExternalVolume {
	var mounted []*ateapipb.ExternalVolume
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *atev1alpha1.Volume
		for _, tVol := range template.Spec.Volumes {
			expectedID := actorVolumeID(ref, tVol.Name)
			if vol.GetActorVolumeId() == expectedID {
				matchedTemplateVol = &tVol
				break
			}
		}

		if matchedTemplateVol == nil {
			slog.WarnContext(ctx, "Volume not found in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}

		if !isVolumeMounted(matchedTemplateVol.Name, template) {
			slog.InfoContext(ctx, "Volume not mounted in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}
		mounted = append(mounted, vol)
	}
	return mounted
}

func actorVolumeID(ref *ateapipb.ObjectRef, volumeName string) string {
	// TODO consider if this should be actor UUID
	return fmt.Sprintf("%s-%s-%s", ref.GetAtespace(), ref.GetName(), volumeName)
}
