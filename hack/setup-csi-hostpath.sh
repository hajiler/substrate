# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Define paths
DRIVER_DIR="${ROOT}/bin/csi-driver-host-path"
DEPLOY_DIR="${DRIVER_DIR}/deploy/kubernetes-1.27"

# 1. Ensure driver is cloned
if [ ! -d "${DRIVER_DIR}" ]; then
  echo "CSI Hostpath Driver not found at ${DRIVER_DIR}. Cloning..."
  git clone https://github.com/kubernetes-csi/csi-driver-host-path.git "${DRIVER_DIR}"
fi

# 2. Clean up existing deployment if present
echo "Cleaning up existing CSI Hostpath resources..."
# Run destroy script, ignore errors if resources already deleted
"${DEPLOY_DIR}/destroy.sh" >/dev/null 2>&1 || true
# Also delete our custom service and SC
kubectl delete service csi-hostpath-controller -n default >/dev/null 2>&1 || true
kubectl delete storageclass csi-hostpath-sc >/dev/null 2>&1 || true

# Also clean up the host directories inside Kind node (best effort)
echo "Cleaning up CSI directories on Kind node..."
KIND_NODE="kind-control-plane"
if docker ps | grep -q "${KIND_NODE}"; then
  # Unmount any stale mounts to prevent "device or resource busy"
  docker exec "${KIND_NODE}" sh -c '
    for mnt in $(mount | grep /var/lib/ateom-gvisor | awk "{print \$3}"); do
      echo "Unmounting stale mount: ${mnt}"
      umount -f "${mnt}" || true
    done
  ' || true
  # Only delete contents, keep the directory itself to preserve mounts of running pods (like atelet)
  docker exec "${KIND_NODE}" sh -c 'rm -rf /var/lib/ateom-gvisor/*' || true
else
  echo "Warning: Kind node ${KIND_NODE} not running. Skipping directory cleanup."
fi

# 3. Deploy the CSI Hostpath Driver
echo "Deploying CSI Hostpath Driver..."
# The script might fail at snapshotclass due to missing CRDs. We catch and ignore this.
"${DEPLOY_DIR}/deploy.sh" || {
  echo "Warning: deploy.sh exited with error (likely VolumeSnapshotClass CRD missing). Checking if plugin StatefulSet is created..."
}

# Verify StatefulSet exists
if ! kubectl get statefulset csi-hostpathplugin -n default >/dev/null 2>&1; then
  echo "Error: csi-hostpathplugin StatefulSet was not created!"
  exit 1
fi

# 4. Patch the CSI Driver to mount Substrate directory
echo "Patching CSI Hostpath StatefulSet..."
kubectl patch statefulset csi-hostpathplugin -n default --patch '
spec:
  template:
    spec:
      containers:
      - name: hostpath
        volumeMounts:
        - name: ateom-dir
          mountPath: /var/lib/ateom-gvisor
          mountPropagation: Bidirectional
      volumes:
      - name: ateom-dir
        hostPath:
          path: /var/lib/ateom-gvisor
          type: DirectoryOrCreate
'

# 5. Deploy Socat Proxy (from testing manifest)
echo "Deploying csi-hostpath-socat proxy..."
kubectl apply -f "${DEPLOY_DIR}/hostpath/csi-hostpath-testing.yaml"

# 6. Create the TCP Service mapping port 50051 to socat port 10000
echo "Exposing CSI Controller over TCP Service..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: csi-hostpath-controller
  namespace: default
spec:
  selector:
    app.kubernetes.io/name: csi-hostpath-socat
  ports:
  - port: 50051
    targetPort: 10000
    name: grpc
EOF

# 7. Create the StorageClass
echo "Creating csi-hostpath-sc StorageClass..."
kubectl apply -f "${DRIVER_DIR}/examples/csi-storageclass.yaml"

# 8. Wait for pods to be ready
echo "Waiting for CSI Hostpath and Proxy pods to be Ready..."
kubectl rollout status statefulset/csi-hostpathplugin -n default --timeout=120s
kubectl rollout status statefulset/csi-hostpath-socat -n default --timeout=120s

echo "CSI Hostpath setup complete!"
