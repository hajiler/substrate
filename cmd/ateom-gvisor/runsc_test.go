//go:build linux

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

package main

import (
	"reflect"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestKillArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.killArgs("my-container", "SIGTERM")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"kill",
		"my-container",
		"SIGTERM",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("killArgs() = %v, want %v", got, want)
	}
}

func TestWaitArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.waitArgs("my-container")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"wait",
		"my-container",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("waitArgs() = %v, want %v", got, want)
	}
}

func TestPauseArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.pauseArgs(ocispec.PauseContainer)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"pause",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("pauseArgs() = %v, want %v", got, want)
	}
}

func TestResumeArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.resumeArgs(ocispec.PauseContainer)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"resume",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("resumeArgs() = %v, want %v", got, want)
	}
}

func TestCreateArgs(t *testing.T) {
	r := &runsc{path: "/usr/bin/runsc", actorUID: "test-actor-123"}

	got := r.createArgs("my-container", []string{"-extra"})
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"--cpu-num-from-quota",
		"create",
		"-bundle", ateompath.OCIBundlePath("test-actor-123", "my-container"),
		"-pid-file", ateompath.PIDFilePath("test-actor-123", "my-container"),
		"-extra",
		"my-container",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("createArgs() = %v, want %v", got, want)
	}
}

func TestRestoreArgs(t *testing.T) {
	r := &runsc{path: "/usr/bin/runsc", actorUID: "test-actor-123"}

	got := r.restoreArgs("my-container", "/ckpt")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"--cpu-num-from-quota",
		"restore",
		"-bundle", ateompath.OCIBundlePath("test-actor-123", "my-container"),
		"-image-path", "/ckpt",
		"-pid-file", ateompath.PIDFilePath("test-actor-123", "my-container"),
		"-background",
		"-detach",
		"my-container",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("restoreArgs() = %v, want %v", got, want)
	}
}

func TestRestoreSpecValidationFlagPrecedesSubcommand(t *testing.T) {
	r := &runsc{path: "/usr/bin/runsc", actorUID: "test-actor-123", restoreSpecValidation: "warning"}

	for name, args := range map[string][]string{
		"create":  r.createArgs(ocispec.PauseContainer, nil),
		"restore": r.restoreArgs(ocispec.PauseContainer, "/ckpt"),
	} {
		flag := slices.Index(args, "--restore-spec-validation=warning")
		sub := slices.Index(args, name)
		if flag < 0 || sub < 0 || flag > sub {
			t.Errorf("%s args = %v; want --restore-spec-validation=warning before the %q subcommand", name, args, name)
		}
	}
	if args := r.killArgs("c", "SIGTERM"); slices.Contains(args, "--restore-spec-validation=warning") {
		t.Errorf("killArgs() = %v; restore policy must not leak into unrelated commands", args)
	}
}

func TestRestoreSpecValidationFor(t *testing.T) {
	csi := &ateompb.WorkloadSpec{Containers: []*ateompb.Container{
		{Name: "a"},
		{Name: "b", CsiVolumeMounts: []*ateompb.VolumeMount{{VolumeName: "shared", MountPath: "/mnt/shared"}}},
	}}
	if got := restoreSpecValidationFor(csi); got != "warning" {
		t.Errorf("restoreSpecValidationFor(csi) = %q, want %q", got, "warning")
	}
	durableOnly := &ateompb.WorkloadSpec{Containers: []*ateompb.Container{
		{Name: "a", DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{VolumeName: "d", MountPath: "/d"}}},
	}}
	if got := restoreSpecValidationFor(durableOnly); got != "" {
		t.Errorf("restoreSpecValidationFor(durable only) = %q, want empty", got)
	}
	if got := restoreSpecValidationFor(nil); got != "" {
		t.Errorf("restoreSpecValidationFor(nil) = %q, want empty", got)
	}
}
