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

package csi

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

type CSINodePlugin struct {
	endpoint string
}

func NewCSINodePlugin(endpoint string) *CSINodePlugin {
	return &CSINodePlugin{
		endpoint: endpoint,
	}
}

// Ensure it implements the interface
var _ volume.VolumePlugin = &CSINodePlugin{}

func parseCapacity(capStr string) (int64, error) {
	var val int64
	var unit string
	_, err := fmt.Sscanf(capStr, "%d%s", &val, &unit)
	if err != nil {
		// Try parsing as just a number
		_, err = fmt.Sscanf(capStr, "%d", &val)
		if err != nil {
			return 0, fmt.Errorf("invalid capacity format: %w", err)
		}
		return val, nil
	}
	switch unit {
	case "Gi", "G":
		return val * 1024 * 1024 * 1024, nil
	case "Mi", "M":
		return val * 1024 * 1024, nil
	case "Ki", "K":
		return val * 1024, nil
	default:
		return val, nil
	}
}

func (p *CSINodePlugin) CreateVolume(ctx context.Context, name string, capacity string, storageClass string) (string, error) {
	slog.InfoContext(ctx, "CSINodePlugin.CreateVolume", slog.String("name", name), slog.String("capacity", capacity))

	capBytes, err := parseCapacity(capacity)
	if err != nil {
		return "", fmt.Errorf("failed to parse capacity %q: %w", capacity, err)
	}

	conn, err := Connect(p.endpoint)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := csi.NewControllerClient(conn)

	req := &csi.CreateVolumeRequest{
		Name: name,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: capBytes,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	}

	resp, err := client.CreateVolume(ctx, req)
	if err != nil {
		return "", fmt.Errorf("CreateVolume failed: %w", err)
	}

	return resp.GetVolume().GetVolumeId(), nil
}

func (p *CSINodePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	slog.InfoContext(ctx, "CSINodePlugin.DeleteVolume", slog.String("volumeID", volumeID))

	conn, err := Connect(p.endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := csi.NewControllerClient(conn)

	req := &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	}

	_, err = client.DeleteVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("DeleteVolume failed: %w", err)
	}

	return nil
}

func (p *CSINodePlugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	slog.InfoContext(ctx, "CSINodePlugin.AttachVolume (stub)", slog.String("volumeID", volumeID), slog.String("node", node))
	return nil
}

func (p *CSINodePlugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	slog.InfoContext(ctx, "CSINodePlugin.DetachVolume (stub)", slog.String("volumeID", volumeID), slog.String("node", node))
	return nil
}

func (p *CSINodePlugin) stagingPathPrefix() string {
	if prefix := os.Getenv("ACTOR_VOLUME_CSI_STAGING_PATH_PREFIX"); prefix != "" {
		return prefix
	}
	return "/var/lib/ateom-gvisor/globalmounts"
}

func (p *CSINodePlugin) MountVolume(ctx context.Context, volumeID string, targetPath string) error {
	slog.InfoContext(ctx, "CSINodePlugin.MountVolume", slog.String("volumeID", volumeID), slog.String("targetPath", targetPath))

	conn, err := Connect(p.endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := csi.NewNodeClient(conn)

	// 1. Stage the volume
	stagingTargetPath := filepath.Join(p.stagingPathPrefix(), volumeID)
	if err := os.MkdirAll(stagingTargetPath, 0750); err != nil {
		return fmt.Errorf("failed to create staging target path %q: %w", stagingTargetPath, err)
	}

	stageReq := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err = client.NodeStageVolume(ctx, stageReq)
	if err != nil {
		return fmt.Errorf("NodeStageVolume failed for volume %q: %w", volumeID, err)
	}

	// 2. Publish the volume
	// Ensure target path exists and is a directory
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		return fmt.Errorf("failed to create target path %q: %w", targetPath, err)
	}

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	_, err = client.NodePublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("NodePublishVolume failed for volume %q: %w", volumeID, err)
	}

	return nil
}

func (p *CSINodePlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	slog.InfoContext(ctx, "CSINodePlugin.UnmountVolume", slog.String("volumeID", volumeID), slog.String("targetPath", targetPath))

	conn, err := Connect(p.endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := csi.NewNodeClient(conn)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 1. Unpublish
	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	}

	_, err = client.NodeUnpublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("NodeUnpublishVolume failed for volume %q: %w", volumeID, err)
	}

	// Clean up target path if it is empty
	if err := os.Remove(targetPath); err != nil {
		slog.DebugContext(ctx, "failed to remove target path", slog.String("path", targetPath), slog.Any("error", err))
	}

	// 2. Unstage
	stagingTargetPath := filepath.Join(p.stagingPathPrefix(), volumeID)
	unstageReq := &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingTargetPath,
	}

	_, err = client.NodeUnstageVolume(ctx, unstageReq)
	if err != nil {
		return fmt.Errorf("NodeUnstageVolume failed for volume %q: %w", volumeID, err)
	}

	// Clean up staging target path if it is empty
	if err := os.Remove(stagingTargetPath); err != nil {
		slog.DebugContext(ctx, "failed to remove staging target path", slog.String("path", stagingTargetPath), slog.Any("error", err))
	}

	return nil
}
