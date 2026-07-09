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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeStageVolume performs initial setup (staging) of a volume (e.g., formatting).
func (c *Client) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.node.NodeStageVolume(ctx, req)
	return nil, status.Error(codes.Unimplemented, "NodeStageVolume is not implemented")
}

// NodeUnstageVolume undoes NodeStageVolume.
func (c *Client) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.node.NodeUnstageVolume(ctx, req)
	return nil, status.Error(codes.Unimplemented, "NodeUnstageVolume is not implemented")
}

// NodePublishVolume mounts the volume to the target path.
func (c *Client) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.node.NodePublishVolume(ctx, req)
	return nil, status.Error(codes.Unimplemented, "NodePublishVolume is not implemented")
}

// NodeUnpublishVolume unmounts the volume.
func (c *Client) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.node.NodeUnpublishVolume(ctx, req)
	return nil, status.Error(codes.Unimplemented, "NodeUnpublishVolume is not implemented")
}
