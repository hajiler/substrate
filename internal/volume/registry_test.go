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

package volume_test

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
)

type dummyPlugin struct{}

func (d *dummyPlugin) CreateVolume(ctx context.Context, name string, capacity string, storageClass string) (string, error) {
	return name, nil
}
func (d *dummyPlugin) DeleteVolume(ctx context.Context, volumeID string) error { return nil }
func (d *dummyPlugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	return nil
}
func (d *dummyPlugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	return nil
}
func (d *dummyPlugin) MountVolume(ctx context.Context, volumeID string, targetPath string) error {
	return nil
}
func (d *dummyPlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	return nil
}

func TestRegistry(t *testing.T) {
	pluginName := "test-dummy"
	dummy := &dummyPlugin{}

	// Test Get before Register
	_, err := volume.GetPlugin(pluginName)
	if err == nil {
		t.Fatalf("expected error getting unregistered plugin, got nil")
	}

	// Test Register
	volume.RegisterPlugin(pluginName, dummy)

	// Test Get after Register
	p, err := volume.GetPlugin(pluginName)
	if err != nil {
		t.Fatalf("unexpected error getting plugin: %v", err)
	}
	if p != dummy {
		t.Fatalf("got wrong plugin: expected %p, got %p", dummy, p)
	}

	// Test Duplicate Register (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on duplicate register, did not panic")
		}
	}()
	volume.RegisterPlugin(pluginName, dummy)
}
