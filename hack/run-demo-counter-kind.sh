#!/usr/bin/env bash

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

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

# ANSI color codes for pretty output
COLOR_CYAN='\033[1;36m'
COLOR_GREEN='\033[1;32m'
COLOR_YELLOW='\033[1;33m'
COLOR_RED='\033[1;31m'
COLOR_RESET='\033[0m'

function log_step() {
  echo -e "${COLOR_CYAN}[step]: $1${COLOR_RESET}"
}

function log_success() {
  echo -e "${COLOR_GREEN}[success]: $1${COLOR_RESET}"
}

function log_warn() {
  echo -e "${COLOR_YELLOW}[warning]: $1${COLOR_RESET}"
}

function log_error() {
  echo -e "${COLOR_RED}[error]: $1${COLOR_RESET}"
}

log_step "Cleaning up previous test"
./hack/delete-kind-cluster.sh || true

log_step "Installing kind cluster"
./hack/create-kind-cluster.sh

log_step "Installing ATE control plane, Valkey, and RustFS..."
./hack/install-ate-kind.sh --deploy-ate-system

log_step "Installing counter demo..."
./hack/install-ate-kind.sh --deploy-demo-counter

log_step "Installing kubectl-ate CLI..."
go install ./cmd/kubectl-ate

log_step "Creating atespace (demo)..."
kubectl ate create atespace demo

log_step "Creating counter actor (my-counter-1)..."
kubectl ate create actor my-counter-1 --template ate-demo-counter/counter --atespace demo

log_success "Counter actor my-counter-1 created"
echo ""
echo -e "${COLOR_YELLOW}========================================================================${COLOR_RESET}"
echo -e "To interact with the counter actor, open a separate terminal and run:"
echo -e "  curl -X POST -H \"Host: my-counter-1.demo.actors.resources.substrate.ate.dev\" -i http://localhost:8000/"
echo -e "${COLOR_YELLOW}========================================================================${COLOR_RESET}"
echo ""
log_step "Starting port-forwarding for the network router (press Ctrl+C to stop)..."
kubectl port-forward -n ate-system svc/atenet-router 8000:80
