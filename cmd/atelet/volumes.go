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
	"github.com/agent-substrate/substrate/pkg/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)


func (s *AteomHerder) mountExternalVolumes(ctx context.Context, atespace, actorID string, volumes []*ateletpb.Volume) error {
	for _, vol := range volumes {
		if vol.GetType() != ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL {
			continue
		}
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		hostPath := ateompath.VolumeHostPath(atespace, actorID, vol.GetName())
		if err := os.MkdirAll(hostPath, 0o750); err != nil {
			return fmt.Errorf("failed to create mount point %q: %w", hostPath, err)
		}
		slog.InfoContext(ctx, "Mounting volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.String("volume_type", ext.GetVolumeType()))
		plugin, err := s.getOrDiscoverPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			return fmt.Errorf("failed to get volume plugin for %q: %w", ext.GetVolumeType(), err)
		}
		if err := plugin.MountVolume(ctx, ext.GetStorageVolumeId(), hostPath, ext.GetVolumeContext()); err != nil {
			return fmt.Errorf("failed to mount volume %q to %q: %w", ext.GetStorageVolumeId(), hostPath, err)
		}
	}
	return nil
}

func (s *AteomHerder) unmountExternalVolumes(ctx context.Context, atespace, actorID string, volumes []*ateletpb.Volume) error {
	for _, vol := range volumes {
		if vol.GetType() != ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL {
			continue
		}
		ext := vol.GetExternal()
		if ext == nil {
			continue
		}
		hostPath := ateompath.VolumeHostPath(atespace, actorID, vol.GetName())
		slog.InfoContext(ctx, "Unmounting volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.String("volume_type", ext.GetVolumeType()))
		plugin, err := s.getOrDiscoverPlugin(ctx, ext.GetVolumeType())
		if err != nil {
			slog.ErrorContext(ctx, "failed to get volume plugin", slog.String("volume_type", ext.GetVolumeType()), slog.String("volume_id", ext.GetStorageVolumeId()), slog.Any("error", err))
			continue
		}
		if err := plugin.UnmountVolume(ctx, ext.GetStorageVolumeId(), hostPath); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during unmount, assuming already unmounted", slog.String("volume_id", ext.GetStorageVolumeId()), slog.Any("error", err))
			} else {
				slog.ErrorContext(ctx, "failed to unmount volume", slog.String("volume_id", ext.GetStorageVolumeId()), slog.String("host_path", hostPath), slog.Any("error", err))
			}
		}
	}
	return nil
}

func (s *AteomHerder) getOrDiscoverPlugin(ctx context.Context, driverName string) (volume.VolumePlugin, error) {
	s.mu.RLock()
	plugin, ok := s.volumePlugins[driverName]
	s.mu.RUnlock()
	if ok {
		return plugin, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double check in case another thread resolved it while we were waiting for lock
	if plugin, ok := s.volumePlugins[driverName]; ok {
		return plugin, nil
	}

	slog.InfoContext(ctx, "Driver not found in map, attempting dynamic discovery", slog.String("driver", driverName))

	var endpoint string
	if s.ateClient != nil {
		// List CSIDriverConfigs to find matching driver
		configs, err := s.ateClient.ApiV1alpha1().CSIDriverConfigs().List(ctx, metav1.ListOptions{})
		if err != nil {
			slog.WarnContext(ctx, "Failed to list CSIDriverConfigs, falling back to default socket", slog.Any("error", err))
		} else {
			for _, cfg := range configs.Items {
				if cfg.Spec.DriverName == driverName {
					// For atelet (node agent), we prefer NodeSocketOverride if present.
					// If NodeSocketOverride is empty, we fall back to ControllerEndpoint.
					if cfg.Spec.NodeSocketOverride != "" {
						endpoint = cfg.Spec.NodeSocketOverride
						slog.InfoContext(ctx, "Found CSIDriverConfig with NodeSocketOverride", slog.String("driver", driverName), slog.String("endpoint", endpoint))
					} else {
						endpoint = cfg.Spec.ControllerEndpoint
						slog.InfoContext(ctx, "Found CSIDriverConfig with ControllerEndpoint (fallback)", slog.String("driver", driverName), slog.String("endpoint", endpoint))
					}
					break
				}
			}
		}
	}

	if endpoint == "" {
		// Default socket path as a function of driverName
		endpoint = fmt.Sprintf("unix:///var/lib/kubelet/plugins/%s/csi.sock", driverName)
		slog.InfoContext(ctx, "No config found, using default socket path", slog.String("driver", driverName), slog.String("endpoint", endpoint))
	}

	csiClient, err := csi.NewCSIClient(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CSI client for %q: %w", driverName, err)
	}
	csiPlugin := csi.NewPlugin(csiClient)

	// Double check the driver name reported by the plugin matches what we expected
	reportedName, err := csiPlugin.DriverName(ctx)
	if err != nil {
		csiClient.Close()
		return nil, fmt.Errorf("failed to get driver name from plugin %q: %w", driverName, err)
	}
	if reportedName != driverName {
		csiClient.Close()
		return nil, fmt.Errorf("reported driver name %q does not match requested name %q", reportedName, driverName)
	}

	s.volumePlugins[driverName] = csiPlugin
	slog.InfoContext(ctx, "Successfully registered dynamic CSI volume plugin", slog.String("driver", driverName), slog.String("endpoint", endpoint))
	return csiPlugin, nil
}
