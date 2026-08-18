# CSI Volumes for Actors in Agent Substrate

Substrate integrates with the **Container Storage Interface (CSI)** to provide dynamically provisioned, per-actor external volumes that seamlessly attach and detach as actors transition through their lifecycle.

---

## 1. CSI in Substrate vs. Standard Kubernetes

In Kubernetes, volume are reconciled asynchronously via standard Kubernetes objects (e.g. `PersistentVolumeClaim`, `PersistentVolume`). Agent Substrate takes a different approach tailored for actor lifecycle operations:

* **No PV or PVC Objects:** External volumes are declaratively defined in the [`ActorTemplate`](api-guide.md#2-actortemplate-the-workload-blueprint) via `externalVolumeTemplate` and provisioned dynamically for each actor instance. Volume operations are coupled directly with the actor lifecycle.
* **Direct Network-Based CSI Controller:** The Substrate control plane (`ateapi`) communicates directly with the CSI Controller gRPC service over the network (via TCP or DNS endpoints).

---

## 2. Dynamic CSI Driver Discovery (`CSIDriverConfig`)

To discovery and communicate with CSI drivers Substrate uses dynamic discovery driven by the cluster-scoped **`CSIDriverConfig`** Custom Resource Definition (CRD).

### The `CSIDriverConfig` Resource

`CSIDriverConfig` defines the gRPC connection parameters for a specific CSI driver. It bridges the Kubernetes `StorageClass` (referenced in the `ActorTemplate`) to the network endpoint of the CSI Controller service and the local socket path of the CSI Node plugin.

```yaml
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
  tls:
    enabled: false
```

### Specification (`CSIDriverConfigSpec`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `driverName` | `string` | **Required.** The standard CSI driver name (e.g. `nfs.csi.k8s.io`, `hostpath.csi.k8s.io`, `pd.csi.storage.gke.io`). Matches the `provisioner` field on the referenced Kubernetes `StorageClass`. |
| `controllerEndpoint` | `string` | **Required.** The gRPC endpoint for the CSI Controller service. Must be a valid URI starting with `tcp://`, `dns:///`, or `unix://` (e.g., `tcp://csi-controller.kube-system.svc:50051` or `dns:///csi-svc.default.svc:9000`). |
| `nodeSocketOverride` | `string` | **Optional.** Override for the CSI Node service Unix domain socket on worker nodes. Must begin with `unix://`. If omitted, Substrate defaults to `unix:///var/lib/kubelet/plugins/<driverName>/csi.sock`. |

### Exposing the CSI Controller over the Network

> [!NOTE]
> **Network Accessibility Requirement:**
> Standard upstream CSI driver controller deployments typically bind their gRPC endpoints only to a local Unix domain socket (e.g. `unix:///csi/csi.sock`) for consumption by in-pod Kubernetes sidecars (`csi-provisioner`, `csi-attacher`).
> 
> Because Substrate's control plane (`ateapi`) communicates directly with the CSI Controller via gRPC over the network, **the CSI Controller service must be exposed over the network via a Kubernetes `Service`**.

#### Example: `csi-nfs` Controller Exposure

In the `csi-nfs` driver deployment (see [`hack/third_party/csi-driver-nfs/deploy/csi-nfs-controller.yaml`](../hack/third_party/csi-driver-nfs/deploy/csi-nfs-controller.yaml) and [`hack/setup-csi-nfs-kind.sh`](../hack/setup-csi-nfs-kind.sh)), the controller socket is exposed over TCP using a proxy sidecar container, and a Kubernetes `Service` provides a stable cluster endpoint:

1. **Proxy Sidecar in `csi-nfs-controller` Deployment:**
   ```yaml
   containers:
   - name: socat
     image: docker.io/alpine/socat:1.7.4.3-r0
     args:
     - tcp-listen:10000,fork,reuseaddr
     - unix-connect:/csi/csi.sock
     securityContext:
       privileged: true
     volumeMounts:
     - mountPath: /csi
       name: socket-dir
   ```

2. **Kubernetes `Service`:**
   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: csi-nfs-controller
     namespace: kube-system
   spec:
     selector:
       app: csi-nfs-controller
     ports:
     - port: 50052
       targetPort: 10000
       name: grpc
   ```

3. **`CSIDriverConfig` Reference:**
   The `controllerEndpoint` in the `CSIDriverConfig` is configured to target this Service:
   ```yaml
   controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
   ```

---

## 3. ActorTemplate: Configuring CSI Volumes

External volumes are declared on the `ActorTemplate` resource. For complete details on actor templates, see the [ActorTemplate: The Workload Blueprint](api-guide.md#2-actortemplate-the-workload-blueprint) section in the Substrate API Guide.

### Volume Configuration Fields

To attach a CSI volume to an actor:

1. Define the volume under `spec.volumes` with an `externalVolumeTemplate`.
2. Mount the volume inside one or more containers under `spec.containers[].volumeMounts`.

#### `spec.volumes[]`

```yaml
volumes:
- name: my-data-volume
  externalVolumeTemplate:
    capacity: 10Gi
    storageClassName: standard-rwx
```

* `name`: Unique DNS-label-compliant volume name.
* `externalVolumeTemplate.capacity`: Quantity string representing the requested volume size (e.g. `1Gi`, `50Gi`).
* `externalVolumeTemplate.storageClassName`: Name of a Kubernetes `StorageClass` present in the cluster whose `provisioner` matches a registered `CSIDriverConfig`.

#### `spec.containers[].volumeMounts[]`

```yaml
volumeMounts:
- name: my-data-volume
  mountPath: /var/data
```

* `name`: Must match the declared `spec.volumes[].name`.
* `mountPath`: Unix path inside the container sandbox where the volume will be mounted.

> [!NOTE]
> All declared volumes in `spec.volumes` must be mounted by at least one container.

---

## 4. End-to-End Example

The following example demonstrates setting up an NFS CSI driver with Substrate and deploying an `ActorTemplate` that mounts an external NFS volume.

### Step 1: Create the StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-nfs-sc
provisioner: nfs.csi.k8s.io
parameters:
  server: nfs-server.default.svc.cluster.local
  share: /
reclaimPolicy: Delete
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=3
  - nolock
```

### Step 2: Register the CSIDriverConfig

```yaml
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: nfs.csi.k8s.io
spec:
  driverName: nfs.csi.k8s.io
  controllerEndpoint: tcp://csi-nfs-controller.kube-system.svc.cluster.local:50052
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
```

### Step 3: Define WorkerPool and ActorTemplate

Refer to [ActorTemplate: The Workload Blueprint](api-guide.md#2-actortemplate-the-workload-blueprint) for general template options.

```yaml
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: agent-pool
  namespace: ate-demo
  labels:
    workload: stateful-agent
spec:
  replicas: 5
  ateomImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor
---
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: stateful-agent-template
  namespace: ate-demo
spec:
  sandboxClass: gvisor
  workerSelector:
    matchLabels:
      workload: stateful-agent
  containers:
  - name: agent
    image: gcr.io/my-project/agent-app@sha256:7f28ab0...
    volumeMounts:
    - name: shared-storage
      mountPath: /mnt/shared
    readyz:
      httpGet:
        path: /readyz
        port: 8080
  snapshotsConfig:
    location: gs://my-snapshots-bucket/stateful-agent
  volumes:
  - name: shared-storage
    externalVolumeTemplate:
      capacity: 5Gi
      storageClassName: csi-nfs-sc
```
