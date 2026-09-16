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

package cmd

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// fakeExternalVolumeClient serves GetExternalVolume from a stored volume and
// records the update it is asked to make.
type fakeExternalVolumeClient struct {
	volume *ateapipb.ExternalVolume

	getCalls  int
	updateReq *ateapipb.UpdateExternalVolumeRequest
}

func (f *fakeExternalVolumeClient) GetExternalVolume(_ context.Context, _ *ateapipb.GetExternalVolumeRequest, _ ...grpc.CallOption) (*ateapipb.ExternalVolume, error) {
	f.getCalls++
	return proto.Clone(f.volume).(*ateapipb.ExternalVolume), nil
}

func (f *fakeExternalVolumeClient) UpdateExternalVolume(_ context.Context, in *ateapipb.UpdateExternalVolumeRequest, _ ...grpc.CallOption) (*ateapipb.ExternalVolume, error) {
	f.updateReq = in
	// Bump the version the way a real server does, so the returned volume is
	// distinguishable from the one the caller sent.
	updated := proto.Clone(in.GetExternalVolume()).(*ateapipb.ExternalVolume)
	updated.Metadata.Version++
	return updated, nil
}

func testExternalVolume() *ateapipb.ExternalVolume {
	return &ateapipb.ExternalVolume{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "space-1",
			Name:     "vol-1",
			Uid:      "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55",
			Version:  1,
		},
		ReclaimPolicy: ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE,
		VolumeId:      "projects/p/zones/z/disks/substrate-8ba9b6ee",
		VolumeType:    "pd.csi.storage.gke.io",
		Status: &ateapipb.ExternalVolumeStatus{
			State: ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY,
		},
	}
}

// TestUpdateExternalVolumeReclaimPolicy checks the read-modify-write wiring:
// the policy change is written against the uid and version that were just
// read, because the server requires both as preconditions.
func TestUpdateExternalVolumeReclaimPolicy(t *testing.T) {
	client := &fakeExternalVolumeClient{volume: testExternalVolume()}
	ref := &ateapipb.ObjectRef{Atespace: "space-1", Name: "vol-1"}
	want := ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN

	resp, err := updateExternalVolumeReclaimPolicy(context.Background(), client, ref, want)
	if err != nil {
		t.Fatalf("updateExternalVolumeReclaimPolicy() error = %v", err)
	}

	if client.getCalls != 1 {
		t.Errorf("GetExternalVolume called %d times, want 1", client.getCalls)
	}

	sent := client.updateReq.GetExternalVolume().GetMetadata()
	if sent.GetUid() != "8ba9b6ee-2e1a-4c0f-9f4e-2f0a0f1c3d55" || sent.GetVersion() != 1 {
		t.Errorf("update sent uid %q version %d, want the values just read", sent.GetUid(), sent.GetVersion())
	}

	wantResp := testExternalVolume()
	wantResp.ReclaimPolicy = want
	wantResp.Metadata.Version = 2
	if diff := cmp.Diff(wantResp, resp, protocmp.Transform()); diff != "" {
		t.Errorf("returned external volume mismatch (-want +got):\n%s", diff)
	}
}

func TestParseReclaimPolicy(t *testing.T) {
	tests := []struct {
		value   string
		want    ateapipb.ReclaimPolicy
		wantErr bool
	}{
		{value: "", want: ateapipb.ReclaimPolicy_RECLAIM_POLICY_UNSPECIFIED},
		{value: "delete", want: ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE},
		{value: "Retain", want: ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN},
		{value: "recycle", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			got, err := parseReclaimPolicy(tc.value)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseReclaimPolicy(%q) error = %v, wantErr %v", tc.value, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("parseReclaimPolicy(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
