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

// GetPluginInfo returns the metadata (name, version) of the CSI plugin.
func (c *Client) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.identity.GetPluginInfo(ctx, req)
	return nil, status.Error(codes.Unimplemented, "GetPluginInfo is not implemented")
}

// GetPluginCapabilities returns the capabilities supported by the CSI plugin.
func (c *Client) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.identity.GetPluginCapabilities(ctx, req)
	return nil, status.Error(codes.Unimplemented, "GetPluginCapabilities is not implemented")
}

// Probe checks the health/readiness status of the CSI plugin.
func (c *Client) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	// Skeleton stub. Replace with actual implementation: return c.identity.Probe(ctx, req)
	return nil, status.Error(codes.Unimplemented, "Probe is not implemented")
}
