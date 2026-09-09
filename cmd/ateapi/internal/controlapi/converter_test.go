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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestSnapshotScopeToAtelet covers the wire scope derivation for template
// content scopes: an unset scope falls back to Full.
func TestSnapshotScopeToAtelet(t *testing.T) {
	tests := []struct {
		name     string
		in       ateapipb.SnapshotContentScope
		expected ateletpb.SnapshotScope
	}{
		{
			name:     "Full scope",
			in:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:     "Data scope",
			in:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:     "Default scope (unspecified)",
			in:       ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED,
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := actorSnapshotContentScopeToAtelet(tt.in)
			if result != tt.expected {
				t.Errorf("actorSnapshotContentScopeToAtelet(%v) = %v, want %v", tt.in, result, tt.expected)
			}
		})
	}
}

// TestEffectiveContentScope pins UNSPECIFIED-means-FULL: stored substrate
// templates may legitimately leave scopes unset.
func TestEffectiveContentScope(t *testing.T) {
	tests := []struct {
		in, expected ateapipb.SnapshotContentScope
	}{
		{ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
	}
	for _, tt := range tests {
		if got := effectiveContentScope(tt.in); got != tt.expected {
			t.Errorf("effectiveContentScope(%v) = %v, want %v", tt.in, got, tt.expected)
		}
	}
}

// TestSandboxClassString pins the label values the scheduler and metrics
// share with the CRD's lower-case enum.
func TestSandboxClassString(t *testing.T) {
	tests := []struct {
		in       ateapipb.SandboxClass
		expected string
	}{
		{ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, "gvisor"},
		{ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, "microvm"},
		{ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED, ""},
	}
	for _, tt := range tests {
		if got := sandboxClassString(tt.in); got != tt.expected {
			t.Errorf("sandboxClassString(%v) = %q, want %q", tt.in, got, tt.expected)
		}
	}
}

// TestAccessModeConversions covers both renderings of the volume access mode.
// An unspecified mode has to become ReadWriteOnce everywhere: a volume
// provisioned before the field existed must keep the mode the driver created
// it under, or a later publish is entitled to fail.
func TestAccessModeConversions(t *testing.T) {
	tests := []struct {
		name       string
		in         ateapipb.VolumeAccessMode
		wantPlugin volume.AccessMode
		wantAtelet ateletpb.VolumeAccessMode
	}{
		{
			name:       "Unspecified defaults to read write once",
			in:         ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_UNSPECIFIED,
			wantPlugin: volume.AccessModeReadWriteOnce,
			wantAtelet: ateletpb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
		},
		{
			name:       "Read write once",
			in:         ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
			wantPlugin: volume.AccessModeReadWriteOnce,
			wantAtelet: ateletpb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_ONCE,
		},
		{
			name:       "Read only many",
			in:         ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
			wantPlugin: volume.AccessModeReadOnlyMany,
			wantAtelet: ateletpb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_ONLY_MANY,
		},
		{
			name:       "Read write many",
			in:         ateapipb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
			wantPlugin: volume.AccessModeReadWriteMany,
			wantAtelet: ateletpb.VolumeAccessMode_VOLUME_ACCESS_MODE_READ_WRITE_MANY,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := accessModeToPlugin(tt.in); got != tt.wantPlugin {
				t.Errorf("accessModeToPlugin(%v) = %q, want %q", tt.in, got, tt.wantPlugin)
			}
			if got := accessModeToAtelet(tt.in); got != tt.wantAtelet {
				t.Errorf("accessModeToAtelet(%v) = %v, want %v", tt.in, got, tt.wantAtelet)
			}
		})
	}
}
