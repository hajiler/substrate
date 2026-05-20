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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/volume"
)

var (
	globalVolumePlugin = volume.NewMockVolumePlugin()
)

// TODO: Replace with actual volume plugin search
func getVolumePlugin() volume.VolumePlugin {
	return globalVolumePlugin
}

func (s *AteomHerder) mountWorkloadVolumes(ctx context.Context, reqNamespace, reqTemplate, reqActorID string, volumes []*ateletpb.Volume) error {
	for _, vol := range volumes {
		hostPath := ateompath.VolumeHostPath(reqNamespace, reqTemplate, reqActorID, vol.GetName())
		if err := os.MkdirAll(hostPath, 0o750); err != nil {
			return fmt.Errorf("failed to create mount point %q: %w", hostPath, err)
		}
		slog.InfoContext(ctx, "Mounting volume", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("host_path", hostPath))
		err := getVolumePlugin().MountVolume(ctx, vol.GetStorageVolumeId(), hostPath)
		if err != nil {
			return fmt.Errorf("failed to mount volume %q to %q: %w", vol.GetStorageVolumeId(), hostPath, err)
		}
	}
	return nil
}

func (s *AteomHerder) unmountWorkloadVolumes(ctx context.Context, reqNamespace, reqTemplate, reqActorID string, volumes []*ateletpb.Volume) error {
	for _, vol := range volumes {
		hostPath := ateompath.VolumeHostPath(reqNamespace, reqTemplate, reqActorID, vol.GetName())
		slog.InfoContext(ctx, "Unmounting volume", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("host_path", hostPath))
		err := getVolumePlugin().UnmountVolume(ctx, vol.GetStorageVolumeId(), hostPath)
		if err != nil {
			slog.ErrorContext(ctx, "failed to unmount volume", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.Any("error", err))
		}
	}
	return nil
}
