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

package csi_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume/csi"
	csispec "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

type mockNodeServer struct {
	csispec.UnimplementedNodeServer
	publishCalled    bool
	unpublishCalled  bool
	stageCalled      bool
	unstageCalled    bool
	lastPublishReq   *csispec.NodePublishVolumeRequest
	lastUnpublishReq *csispec.NodeUnpublishVolumeRequest
	lastStageReq     *csispec.NodeStageVolumeRequest
	lastUnstageReq   *csispec.NodeUnstageVolumeRequest
}

func (m *mockNodeServer) NodeStageVolume(ctx context.Context, req *csispec.NodeStageVolumeRequest) (*csispec.NodeStageVolumeResponse, error) {
	m.stageCalled = true
	m.lastStageReq = req
	return &csispec.NodeStageVolumeResponse{}, nil
}

func (m *mockNodeServer) NodeUnstageVolume(ctx context.Context, req *csispec.NodeUnstageVolumeRequest) (*csispec.NodeUnstageVolumeResponse, error) {
	m.unstageCalled = true
	m.lastUnstageReq = req
	return &csispec.NodeUnstageVolumeResponse{}, nil
}

func (m *mockNodeServer) NodePublishVolume(ctx context.Context, req *csispec.NodePublishVolumeRequest) (*csispec.NodePublishVolumeResponse, error) {
	m.publishCalled = true
	m.lastPublishReq = req
	return &csispec.NodePublishVolumeResponse{}, nil
}

func (m *mockNodeServer) NodeUnpublishVolume(ctx context.Context, req *csispec.NodeUnpublishVolumeRequest) (*csispec.NodeUnpublishVolumeResponse, error) {
	m.unpublishCalled = true
	m.lastUnpublishReq = req
	return &csispec.NodeUnpublishVolumeResponse{}, nil
}

func TestCSINodePlugin(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "csi-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Setenv("ACTOR_VOLUME_CSI_STAGING_PATH_PREFIX", filepath.Join(tmpDir, "globalmounts"))

	socketPath := filepath.Join(tmpDir, "csi.sock")
	endpoint := "unix://" + socketPath

	// Start mock gRPC server
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to listen on socket %q: %v", socketPath, err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	mockNode := &mockNodeServer{}
	csispec.RegisterNodeServer(grpcServer, mockNode)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc server error: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()

	plugin := csi.NewCSINodePlugin(endpoint)
	ctx := context.Background()
	volumeID := "test-vol-1"
	targetPath := filepath.Join(tmpDir, "mount-target")

	// Test MountVolume
	err = plugin.MountVolume(ctx, volumeID, targetPath)
	if err != nil {
		t.Fatalf("MountVolume failed: %v", err)
	}

	if !mockNode.stageCalled {
		t.Errorf("expected NodeStageVolume to be called")
	} else {
		req := mockNode.lastStageReq
		if req.VolumeId != volumeID {
			t.Errorf("expected stage volume ID %q, got %q", volumeID, req.VolumeId)
		}
		expectedStagingPath := filepath.Join(tmpDir, "globalmounts", volumeID)
		if req.StagingTargetPath != expectedStagingPath {
			t.Errorf("expected staging target path %q, got %q", expectedStagingPath, req.StagingTargetPath)
		}
	}

	if !mockNode.publishCalled {
		t.Errorf("expected NodePublishVolume to be called")
	} else {
		req := mockNode.lastPublishReq
		if req.VolumeId != volumeID {
			t.Errorf("expected volume ID %q, got %q", volumeID, req.VolumeId)
		}
		if req.TargetPath != targetPath {
			t.Errorf("expected target path %q, got %q", targetPath, req.TargetPath)
		}
	}

	// Test UnmountVolume
	err = plugin.UnmountVolume(ctx, volumeID, targetPath)
	if err != nil {
		t.Fatalf("UnmountVolume failed: %v", err)
	}

	if !mockNode.unpublishCalled {
		t.Errorf("expected NodeUnpublishVolume to be called")
	} else {
		req := mockNode.lastUnpublishReq
		if req.VolumeId != volumeID {
			t.Errorf("expected volume ID %q, got %q", volumeID, req.VolumeId)
		}
		if req.TargetPath != targetPath {
			t.Errorf("expected target path %q, got %q", targetPath, req.TargetPath)
		}
	}

	if !mockNode.unstageCalled {
		t.Errorf("expected NodeUnstageVolume to be called")
	} else {
		req := mockNode.lastUnstageReq
		if req.VolumeId != volumeID {
			t.Errorf("expected unstage volume ID %q, got %q", volumeID, req.VolumeId)
		}
		expectedStagingPath := filepath.Join(tmpDir, "globalmounts", volumeID)
		if req.StagingTargetPath != expectedStagingPath {
			t.Errorf("expected staging target path %q, got %q", expectedStagingPath, req.StagingTargetPath)
		}
	}
}
