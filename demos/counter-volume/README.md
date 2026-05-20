# Counter Volume Demo

This directory contains a demo of a stateful counter application running on Agent Substrate, with support for `ExternalVolumeTemplate`.

It deploys a simple Go HTTP server (`counter-volume.go`) that increments a counter on every request and preserves state across suspends and resumes. It also mounts an external volume at `/data` and writes a test file to verify volume persistence and mounts.

## Prerequisites

- A k8s cluster with Agent Substrate installed (`./hack/install-ate.sh --deploy-ate-system`).
- `ko` installed for building images.
- A GCS bucket for storing snapshots (configured via `BUCKET_NAME` env var).

## How to Run on Agent Substrate

### 1. Build and Deploy

Use the core installation script to build the image and apply the resolved manifests to your cluster:

```bash
./hack/install-ate.sh --deploy-demo-counter-volume
```

Wait until the template is ready:
```bash
kubectl wait --for=condition=Ready actortemplate/counter-volume -n ate-demo-counter-volume --timeout=5m
```

### 2. Create a Counter Actor

Use `kubectl ate` to create an instance of the counter actor:

```bash
# Create the actor using the counter-volume template.
kubectl ate create actor my-counter-volume-1 --template ate-demo-counter-volume/counter-volume
```

### 3. Verify Volume Creation

Check the status of the actor. You should see the volume info inside the Actor resource:
```bash
kubectl ate get actor my-counter-volume-1
```
The output should contain the volumes list with status `CREATED`.

### 4. How to Uninstall

To remove the counter volume demo resources from your cluster, run:

```bash
./hack/install-ate.sh --delete-demo-counter-volume
```
