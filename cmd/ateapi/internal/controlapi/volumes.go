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

	"github.com/agent-substrate/substrate/internal/volume"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	globalVolumePlugin = volume.NewMockVolumePlugin()
)

// TODO: Replace with actual volume plugin search
func getVolumePlugin() volume.VolumePlugin {
	return globalVolumePlugin
}

// TODO: we should persist creation first so that we can handle background cleanup.
// this probably requires us to add a PROVISIONING actor state.

// createActorVolumes provisions external volumes specified in the actor template.
// It returns the list of created external volumes, or an error if any creation fails.
// If any volume creation fails, it cleans up any volumes created in this call on a best-effort basis.
func (s *Service) createActorVolumes(ctx context.Context, actorID string, template *atev1alpha1.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.Spec.Volumes {
		if vol.ExternalVolumeTemplate != nil {
			// Use a unique name for the volume to ensure idempotency
			uniqueVolName := fmt.Sprintf("%s-%s", actorID, vol.Name)
			storageVolumeID, err := getVolumePlugin().CreateVolume(ctx, uniqueVolName, vol.ExternalVolumeTemplate.Capacity.String(), vol.ExternalVolumeTemplate.StorageClassName)
			if err != nil {
				// TODO: need better system - best effort cleanup of already created volumes
				s.deleteActorVolumes(ctx, actorID, volumes)
				return nil, status.Errorf(codes.Internal, "failed to create volume %q: %v", vol.Name, err)
			}
			volumes = append(volumes, &ateapipb.ExternalVolume{
				ActorVolumeId:   uniqueVolName,
				StorageVolumeId: storageVolumeID,
				VolumeType:      "mock", // TODO fix when we support multiple plugins
				Status:          ateapipb.ExternalVolume_CREATED,
			})
		}
	}
	return volumes, nil
}

// deleteActorVolumes deletes all external volumes in the list on a best-effort basis.
func (s *Service) deleteActorVolumes(ctx context.Context, actorID string, volumes []*ateapipb.ExternalVolume) {
	for _, vol := range volumes {
		if err := getVolumePlugin().DeleteVolume(ctx, vol.GetStorageVolumeId()); err != nil {
			slog.ErrorContext(ctx, "failed to delete volume",
				slog.String("actor_id", actorID),
				slog.String("volume_id", vol.GetStorageVolumeId()),
				slog.Any("error", err))
		}
	}
}
