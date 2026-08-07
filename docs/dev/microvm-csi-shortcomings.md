# Shortcomings of MicroVM CSI Volume Support

This document details the analysis of the "out-of-the-box" status of CSI volume support for microVM actors in Agent Substrate, and identifies the gaps that prevent it from working.

## Verified Failure

1.  **Admission Validation**: The `ActorTemplate` CRD contains a validation rule that explicitly blocks `externalVolumeTemplate` when `sandboxClass` is `microvm`:
    ```yaml
    - message: ExternalVolumes are not supported when sandboxClass is 'microvm'
      rule: '!has(self.sandboxClass) || self.sandboxClass != ''microvm'' || !has(self.volumes) || !self.volumes.exists(v, has(v.externalVolumeTemplate))'
    ```
    Attempting to register a template with `externalVolumeTemplate` and `sandboxClass: microvm` fails with:
    ```
    The ActorTemplate "counter-microvm-csi" is invalid: spec: Invalid value: ExternalVolumes are not supported when sandboxClass is 'microvm'
    ```

2.  **Runtime Silence (after bypassing validation)**: After bypassing the validation rule, the template registers, and the actor can be resumed. However:
    *   The CSI volume is mounted on the host by `atelet` (verified in `atelet` logs).
    *   The volume is **not** exposed to the microVM.
    *   The container inside the microVM starts successfully but writes to the guest's local RAM-backed filesystem instead of the CSI volume.
    *   This was verified by checking the host directory for the CSI volume (`/var/lib/ateom-gvisor/actors/<actor-uid>/volumes/data` on the Kind node), which remained empty even after the actor reported successful writes.

3.  **Checkpoint Failure**: Attempting to suspend the actor fails with:
    ```
    rpc error: code = FailedPrecondition desc = no durable-dir volumes found for a Data-scope snapshot
    ```
    This is because the runtime does not recognize the external volume as a durable volume, and thus refuses to perform a Data-scope checkpoint.

## Cause Analysis

The root cause is that `cmd/ateom-microvm` only supports `durableDir` volumes and ignores `externalVolumeTemplate` volumes.

Specifically:

1.  **Volume Detection**: `cmd/ateom-microvm/durable.go` uses `hasDurableVolumes` which only checks for `durableDir` volume mounts:
    ```go
    func hasDurableVolumes(containers []*ateompb.Container) bool {
        for _, c := range containers {
            if len(c.GetDurableDirVolumeMounts()) > 0 { // This only matches durableDir
                return true
            }
        }
        return false
    }
    ```
    `externalVolumeTemplate` volumes are mapped to different fields in the proto (likely `VolumeSource.External` or similar, need to verify proto definition).

2.  **Virtiofsd and VMM Config**: Because `hasDurableVolumes` returns `false`:
    *   `stageDurableShare` is not called, so no `virtiofsd` is started for the volumes directory.
    *   `buildVMConfig` is called with `withDurable = false`, so the second `virtio-fs` device (tagged `kata.DurableFsTag`) is not added to the Cloud-Hypervisor configuration.

3.  **Guest Mounts**: `CreateSandboxForActor` does not mount the durable share in the guest because `withDurableShare` is false.

4.  **OCI Spec Mapping**: `workloadSpec` does not append the volume mounts to the container's OCI spec because `c.durableMounts` is empty.

## Technical Solution

To enable CSI volume support for microVM actors, we implemented the following changes:

1.  **Bypass Admission Validation**:
    *   Modified `pkg/api/v1alpha1/actortemplate_types.go` to comment out the validation rule blocking `externalVolumeTemplate` for `microvm`.
    *   Regenerated CRDs using `go generate`.

2.  **Proto Updates**:
    *   Updated `internal/proto/ateompb/ateom.proto` to add `csi_volume_mounts` field to `Container` message using a new `VolumeMount` message.
    *   Regenerated proto files.

3.  **Atelet changes**:
    *   Modified `cmd/atelet/main.go` (`buildAteomWorkloadSpec`) to translate `VOLUME_TYPE_EXTERNAL` volumes from `ateletpb` to the new `csi_volume_mounts` in `ateompb`.
    *   Relaxed `Checkpoint` validation in `cmd/atelet/main.go` to allow empty `SnapshotFiles` list when checkpointing in `DATA` scope if the actor only has CSI volumes (as they don't require taring).

4.  **Ateom MicroVM Runtime changes**:
    *   **Virtiofsd Staging**: Implemented `stageCsiShare` in a new file `cmd/ateom-microvm/csi.go` to start a separate `virtiofsd` instance targeting the host CSI volumes directory (`/var/lib/ateom-gvisor/actors/<actor-uid>/volumes/`).
    *   **VMM Config**: Updated `buildVMConfig` and `buildFsConfigs` in `run.go` to add a new `virtio-fs` device with tag `ateCsi` pointing to the CSI virtiofsd socket.
    *   **Guest Mount**: Updated `CreateSandboxForActor` (in `internal/kata`) to mount the `ateCsi` share in the guest at `/run/ateom-csi`.
    *   **OCI Spec Integration**: Updated `workloadSpec` (in `durable.go`) to append CSI volume mounts (pointing to guest `/run/ateom-csi/<volume-name>`) to the container's OCI spec.
    *   **Restore Support**: Updated `RestoreWorkload` in `restore.go` to stage the CSI share on restore, and updated `rewriteSnapshotSocketPaths` to handle the `ateCsi` tag.
    *   **Checkpoint Support**: Updated `CheckpointWorkload` in `checkpoint.go` to permit `DATA` scope checkpoints for CSI-only actors.
    *   **Resource Cleanup**: Updated `runningActor` and `teardownActor` to track and kill the CSI `virtiofsd` process.

## Verification Results

We verified the implementation using the `counter-microvm-csi` test template:

1.  **Successful Resume and Write**:
    *   The actor resumed successfully.
    *   Curling the actor returned `preserved file counter: 1`.
    *   Verified on the Kind node that `/var/lib/ateom-gvisor/actors/<actor-uid>/volumes/data/a.txt` was created and contained `1`.
    *   Subsequent curl incremented the counter to `2` on the host file.

2.  **Successful Suspend (DATA scope)**:
    *   Suspending the actor succeeded:
        ```
        go run ./cmd/kubectl-ate suspend actor my-counter -a demo-csi
        ```
    *   The actor status transitioned to `STATUS_SUSPENDED`.

3.  **Data Persistence across Resume**:
    *   Resuming the actor after suspend (via curl) triggered a cold boot.
    *   The guest memory counter reset to `1`.
    *   The file counter continued from the persisted value and returned `3`:
        ```
        hello from: 169.254.17.2 | preserved memory count: 1 | preserved file counter: 3
        ```
    *   Verified the host file `a.txt` was updated to `3`.

## Modified Files

*   `pkg/api/v1alpha1/actortemplate_types.go` (Bypassed validation)
*   `manifests/ate-install/generated/ate.dev_actortemplates.yaml` (Regenerated CRD)
*   `internal/proto/ateompb/ateom.proto` (Added CSI mounts to proto)
*   `internal/proto/ateompb/ateom.pb.go` (Regenerated)
*   `cmd/atelet/main.go` (Populated proto fields, relaxed checkpoint validation)
*   `cmd/ateom-microvm/csi.go` (New file, CSI helper functions)
*   `cmd/ateom-microvm/run.go` (Integrate CSI share, update VMM config and sandbox creation)
*   `cmd/ateom-microvm/restore.go` (Stage CSI share on restore, rewrite socket paths)
*   `cmd/ateom-microvm/checkpoint.go` (Allow CSI checkpoints, teardown virtiofsd)
*   `cmd/ateom-microvm/durable.go` (Append CSI mounts to OCI spec)
*   `cmd/ateom-microvm/internal/kata/overlay_linux.go` (Add guest mount logic for CSI)
*   `cmd/ateom-microvm/internal/kata/restore.go` (Add CSI socket path helper)
