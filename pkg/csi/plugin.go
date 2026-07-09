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

package csi

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Plugin implements volume.VolumePlugin using the CSI Client.
type Plugin struct {
	client *Client
}

// Ensure Plugin implements volume.VolumePlugin
var _ volume.VolumePlugin = (*Plugin)(nil)

// NewPlugin creates a new Plugin adapter.
func NewPlugin(client *Client) *Plugin {
	return &Plugin{
		client: client,
	}
}

// CreateVolume maps to CSI Controller CreateVolume.
func (p *Plugin) CreateVolume(ctx context.Context, name string, capacity string, storageClass string) (string, error) {
	qty, err := resource.ParseQuantity(capacity)
	if err != nil {
		return "", fmt.Errorf("failed to parse capacity %q: %w", capacity, err)
	}
	capBytes := qty.Value()

	req := &csi.CreateVolumeRequest{
		Name: name,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: capBytes,
		},
		VolumeCapabilities: getStandardCapabilities(),
		Parameters: map[string]string{
			"storageClass": storageClass,
		},
	}

	resp, err := p.client.CreateVolume(ctx, req)
	if err != nil {
		return "", fmt.Errorf("CSI CreateVolume failed: %w", err)
	}

	if resp.GetVolume() == nil {
		return "", fmt.Errorf("CSI CreateVolume response returned nil volume")
	}

	return resp.GetVolume().GetVolumeId(), nil
}

// DeleteVolume maps to CSI Controller DeleteVolume.
func (p *Plugin) DeleteVolume(ctx context.Context, volumeID string) error {
	req := &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	}

	_, err := p.client.DeleteVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI DeleteVolume failed: %w", err)
	}
	return nil
}

// AttachVolume maps to CSI Controller ControllerPublishVolume.
func (p *Plugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	req := &csi.ControllerPublishVolumeRequest{
		VolumeId:         volumeID,
		NodeId:           node,
		VolumeCapability: getStandardCapabilities()[0], // Use primary capability
		Readonly:         false,
	}

	resp, err := p.client.ControllerPublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI ControllerPublishVolume failed: %w", err)
	}

	// NOTE: CSI ControllerPublishVolume returns PublishContext (metadata needed for mounting).
	// Currently, Substrate VolumePlugin interface does not support returning PublishContext.
	// We might need to store this context if the driver requires it (e.g. AWS EBS attachment info).
	_ = resp.GetPublishContext()

	return nil
}

// DetachVolume maps to CSI Controller ControllerUnpublishVolume.
func (p *Plugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	req := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   node,
	}

	_, err := p.client.ControllerUnpublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI ControllerUnpublishVolume failed: %w", err)
	}
	return nil
}

// MountVolume maps to CSI Node NodePublishVolume.
// Note: NodeStageVolume staging is currently not handled here.
func (p *Plugin) MountVolume(ctx context.Context, volumeID string, targetPath string) error {
	req := &csi.NodePublishVolumeRequest{
		VolumeId:         volumeID,
		TargetPath:       targetPath,
		VolumeCapability: getStandardCapabilities()[0],
		Readonly:         false,
		// PublishContext: we would pass the PublishContext retrieved during Attach here.
	}

	_, err := p.client.NodePublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI NodePublishVolume failed: %w", err)
	}
	return nil
}

// UnmountVolume maps to CSI Node NodeUnpublishVolume.
func (p *Plugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	}

	_, err := p.client.NodeUnpublishVolume(ctx, req)
	if err != nil {
		return fmt.Errorf("CSI NodeUnpublishVolume failed: %w", err)
	}
	return nil
}

// Helper to provide standard capabilities for general volume operations.
func getStandardCapabilities() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
		},
	}
}
