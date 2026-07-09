#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

# ANSI color codes for pretty output
COLOR_CYAN='\033[1;36m'
COLOR_GREEN='\033[1;32m'
COLOR_RED='\033[1;31m'
COLOR_RESET='\033[0m'

function log_step() {
  echo -e "${COLOR_CYAN}[step]: $1${COLOR_RESET}"
}

function log_success() {
  echo -e "${COLOR_GREEN}[success]: $1${COLOR_RESET}"
}

function log_error() {
  echo -e "${COLOR_RED}[error]: $1${COLOR_RESET}"
}

CLUSTER_NAME="substrate-beta"
CLEANUP_ON_EXIT=false

# Parse flags
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --cleanup-on-exit) CLEANUP_ON_EXIT=true ;;
    --cluster) CLUSTER_NAME="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done

# Ensure cleanup on exit if requested
function cleanup() {
  if [ "${CLEANUP_ON_EXIT}" = true ]; then
    log_step "Cleaning up cluster..."
    ./hack/delete-kind-cluster.sh || true
  fi
  # Kill port forward
  if [[ -n "${PF_PID:-}" ]]; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

log_step "Cleaning up previous test"
./hack/delete-kind-cluster.sh || true

log_step "Installing kind cluster ${CLUSTER_NAME}..."
export KIND_CLUSTER_NAME="${CLUSTER_NAME}"
./hack/create-kind-cluster.sh

log_step "Deploying CSI hostpath driver..."
CSI_DIR="${ROOT}/bin/csi-driver-host-path"
if [ -d "${CSI_DIR}" ]; then
  rm -rf "${CSI_DIR}"
fi
git clone --depth 1 --branch v1.13.0 https://github.com/kubernetes-csi/csi-driver-host-path.git "${CSI_DIR}"

log_step "Installing VolumeSnapshot CRDs..."
kubectl apply -f "${ROOT}/hack/csi-crds/snapshot.storage.k8s.io_volumesnapshotclasses.yaml"
kubectl apply -f "${ROOT}/hack/csi-crds/snapshot.storage.k8s.io_volumesnapshotcontents.yaml"
kubectl apply -f "${ROOT}/hack/csi-crds/snapshot.storage.k8s.io_volumesnapshots.yaml"

log_step "Running CSI deploy script..."
DEPLOY_DIR=""
# Prefer 1.27 as we know it exists and latest might be a broken symlink
if [ -d "${CSI_DIR}/deploy/kubernetes-1.27" ] && [ -f "${CSI_DIR}/deploy/kubernetes-1.27/deploy.sh" ]; then
  DEPLOY_DIR="${CSI_DIR}/deploy/kubernetes-1.27"
elif [ -d "${CSI_DIR}/deploy/kubernetes-latest" ] && [ -f "${CSI_DIR}/deploy/kubernetes-latest/deploy.sh" ]; then
  DEPLOY_DIR="${CSI_DIR}/deploy/kubernetes-latest"
else
  # Fallback to finding any deploy.sh
  DEPLOY_SH=$(find "${CSI_DIR}/deploy" -name deploy.sh | head -n 1)
  if [ -n "${DEPLOY_SH}" ]; then
    DEPLOY_DIR=$(dirname "${DEPLOY_SH}")
  fi
fi

if [ -n "${DEPLOY_DIR}" ]; then
  log_step "Found deploy directory: ${DEPLOY_DIR}"
  (cd "${DEPLOY_DIR}" && export INSTALL_CRD=true && bash ./deploy.sh)
else
  log_error "Could not find deploy.sh in CSI driver repo"
  exit 1
fi

log_step "Patching CSI hostpath driver to mount /var/lib/ateom-gvisor..."
kubectl patch statefulset csi-hostpathplugin -n default --type='strategic' -p '{"spec":{"template":{"spec":{"containers":[{"name":"hostpath","volumeMounts":[{"name":"ateom-gvisor","mountPath":"/var/lib/ateom-gvisor","mountPropagation":"Bidirectional"}]}],"volumes":[{"name":"ateom-gvisor","hostPath":{"path":"/var/lib/ateom-gvisor","type":"DirectoryOrCreate"}}]}}}}'

log_step "Waiting for CSI hostpath driver to be ready..."
# Wait for pods with label app=csi-hostpathplugin
# They might be in kube-system or default. The deploy script usually prints where.
# Let's wait in both namespaces just in case.
namespaces=("default" "kube-system")
for ns in "${namespaces[@]}"; do
  if kubectl get pods -n "${ns}" -l app=csi-hostpathplugin 2>/dev/null | grep -q csi; then
    log_step "Found CSI pods in namespace ${ns}, waiting for readiness..."
    kubectl wait --namespace "${ns}" \
      --for=condition=ready pod \
      --selector=app=csi-hostpathplugin \
      --timeout=300s
    break
  fi
done

log_step "Installing ATE control plane..."
./hack/install-ate-kind.sh --deploy-ate-system

log_step "Waiting for rustfs storage backend initialization..."
kubectl wait --for=condition=complete --timeout=120s job/rustfs-bucket-init -n ate-system

log_step "Enabling CSI plugin in ATE..."
# Update API server configmap to use csi
kubectl patch configmap ate-api-server-envvars -n ate-system --type merge -p '{"data":{"ACTOR_VOLUME_PLUGIN":"csi"}}'
kubectl rollout restart deployment/ate-api-server-deployment -n ate-system

# Update atelet daemonset to use csi
kubectl set env daemonset/atelet -n ate-system ACTOR_VOLUME_PLUGIN=csi
kubectl rollout status daemonset/atelet -n ate-system --timeout=120s
kubectl rollout status deployment/ate-api-server-deployment -n ate-system --timeout=120s

log_step "Installing counter-volume demo..."
./hack/install-ate-kind.sh --deploy-demo-counter-volume

log_step "Installing kubectl-ate CLI..."
go install ./cmd/kubectl-ate
export GOPATH="${GOPATH:-$(go env GOPATH)}"
export GOBIN="${GOBIN:-$(go env GOBIN)}"
export PATH="${GOBIN}:${GOPATH}/bin:${PATH}"

log_step "Waiting for ActorTemplate counter-volume to be Ready..."
timeout=180
elapsed=0
while true; do
  phase=$(kubectl get actortemplate counter-volume -n ate-demo-counter-volume -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  echo "Template phase: ${phase} (${elapsed}s/${timeout}s)"
  if [ "${phase}" = "Ready" ]; then
    break
  fi
  if [ "${phase}" = "Failed" ]; then
    log_error "Template transitioned to Failed phase"
    exit 1
  fi
  if [ ${elapsed} -ge ${timeout} ]; then
    log_error "Timed out waiting for template to be Ready"
    exit 1
  fi
  sleep 5
  elapsed=$((elapsed + 5))
done

actorID="my-counter-volume-csi"
log_step "Creating Actor ${actorID}..."
kubectl ate create actor "${actorID}" --template ate-demo-counter-volume/counter-volume

# Verify Actor is SUSPENDED initially
status=$(kubectl ate get actor "${actorID}" -o json | jq -r '.actors[0].status')
echo "Actor status: ${status}"
if [ "${status}" != "STATUS_SUSPENDED" ]; then
  log_error "Expected actor status to be STATUS_SUSPENDED, got ${status}"
  exit 1
fi

# Start port forward in background to send requests
log_step "Starting port-forward..."
kubectl port-forward -n ate-system svc/atenet-router 8000:80 &
PF_PID=$!
sleep 2

# Send first request. It should trigger Resume (and Mount).
log_step "Sending first request (triggers resume)..."
resp1=$(curl -s -H "Host: ${actorID}.actors.resources.substrate.ate.dev" http://localhost:8000/)
echo "Response 1: ${resp1}"
if [[ ! "${resp1}" == *"error-reading-file"* ]]; then
  log_error "Expected first request to fail reading file (since it doesn't exist yet)"
  exit 1
fi

# Send second request.
log_step "Sending second request..."
resp2=$(curl -s -H "Host: ${actorID}.actors.resources.substrate.ate.dev" http://localhost:8000/)
echo "Response 2: ${resp2}"
if [[ ! "${resp2}" == *"file content: 1"* ]]; then
  log_error "Expected file content to be '1'"
  exit 1
fi

# Verify mount exists on the node
log_step "Verifying mount on the node..."
NODE_NAME="substrate-beta-control-plane"
mount_path="/var/lib/ateom-gvisor/actors/ate-demo-counter-volume:counter-volume:${actorID}/volumes/data-vol"
if ! docker exec "${NODE_NAME}" mountpoint "${mount_path}"; then
  log_error "Mount point ${mount_path} does not exist on node"
  exit 1
fi
docker exec "${NODE_NAME}" ls -la "${mount_path}"

# Suspend the actor
log_step "Suspending actor..."
kubectl ate pause actor "${actorID}"
# Wait for it to be suspended
until [ "$(kubectl ate get actor "${actorID}" -o json | jq -r '.actors[0].status')" = "STATUS_PAUSED" ]; do
    echo "Waiting for suspension..."
    sleep 2
done

# Verify mount is gone (unmounted)
log_step "Verifying mount is gone after suspend..."
if docker exec "${NODE_NAME}" mountpoint "${mount_path}" >/dev/null 2>&1; then
    log_error "Mount still exists after suspend!"
    exit 1
else
    log_success "Mount is gone."
fi

storage_volume_id=$(kubectl ate get actor "${actorID}" -o json | jq -r '.actors[0].volumes[0].storageVolumeId')
backing_dir="/var/lib/csi-hostpath-data/${storage_volume_id}"
log_step "Checking backing directory content while paused (storageVolumeId: ${storage_volume_id})..."
docker exec "${NODE_NAME}" ls -la "${backing_dir}"
docker exec "${NODE_NAME}" cat "${backing_dir}/random-content-file" || true

# Resume the actor (by sending another request)
log_step "Sending third request (triggers resume again)..."
resp3=$(curl -s -H "Host: ${actorID}.actors.resources.substrate.ate.dev" http://localhost:8000/)
echo "Response 3: ${resp3}"
# The file content should persist and show the previous value (which was 2 after the second request's write,
# wait, the second request writes '2' to the file (requestCount was incremented to 2, and then updated).
# Let's trace:
# Request 1: count=1, reads file (fails -> error), writes count (1) to file.
# Request 2: count=2, reads file (finds 1), writes count (2) to file.
# Request 3: count=3 (if memory preserved) or count=1 (if reset).
# But file content should be what was written in Request 2, which is "2".
# Let's check if resp3 has "file content: 2".
if [[ ! "${resp3}" == *"file content: 2"* ]]; then
  log_error "Expected file content to be '2' after resume, indicating data persisted"
  exit 1
fi

# Verify mount is back
log_step "Verifying mount is back after resume..."
if ! docker exec "${NODE_NAME}" mountpoint "${mount_path}"; then
  log_error "Mount point ${mount_path} does not exist after resume"
  exit 1
fi

# Suspend actor before deletion (API requires actor to be suspended to delete)
log_step "Suspending actor before deletion..."
kubectl ate suspend actor "${actorID}"
until [ "$(kubectl ate get actor "${actorID}" -o json | jq -r '.actors[0].status')" = "STATUS_SUSPENDED" ]; do
    echo "Waiting for suspension..."
    sleep 2
done

# Delete actor
log_step "Deleting actor..."
kubectl ate delete actor "${actorID}"

# Verify cleanup (mount gone and backing directory gone)
log_step "Verifying cleanup..."
sleep 5
if docker exec "${NODE_NAME}" ls -d "${mount_path}" >/dev/null 2>&1; then
    log_error "Mount directory still exists after delete!"
    exit 1
else
    log_success "Mount directory cleaned up."
fi

backing_dir="/var/lib/csi-hostpath-data/${storage_volume_id}"
if docker exec "${NODE_NAME}" ls -d "${backing_dir}" >/dev/null 2>&1; then
    log_error "Backing directory still exists after delete!"
    exit 1
else
    log_success "Backing directory cleaned up."
fi

log_success "CSI Integration Demo Completed Successfully!"
