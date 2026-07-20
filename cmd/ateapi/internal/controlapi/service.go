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
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/volume"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/csi"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/client-go/kubernetes"
)

// Service implements ateapipb.Control
type Service struct {
	ateapipb.UnimplementedControlServer
	persistence           store.Interface
	dialer                *AteletDialer
	actorTemplateLister   listersv1alpha1.ActorTemplateLister
	workerPoolLister      listersv1alpha1.WorkerPoolLister
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister
	actorWorkflow         *ActorWorkflow
	
	mu            sync.RWMutex
	volumePlugins map[string]volume.VolumePlugin
	kubeClient    kubernetes.Interface
}

var _ ateapipb.ControlServer = (*Service)(nil)

// PluginRegistry defines the interface for dynamic CSI plugin resolution.
type PluginRegistry interface {
	GetPlugin(ctx context.Context, name string) (volume.VolumePlugin, error)
}

// NewService creates a service.
func NewService(
	persistence store.Interface,
	workerCache *workercache.Cache,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister,
	dialer *AteletDialer,
	kubeClient kubernetes.Interface,
	volumePlugins map[string]volume.VolumePlugin,
) *Service {
	s := &Service{
		persistence:           persistence,
		actorTemplateLister:   actorTemplateLister,
		workerPoolLister:      workerPoolLister,
		csiDriverConfigLister: csiDriverConfigLister,
		dialer:                dialer,
		volumePlugins:         volumePlugins,
		kubeClient:            kubeClient,
	}
	s.actorWorkflow = NewActorWorkflow(persistence, workerCache, dialer, actorTemplateLister, workerPoolLister, sandboxConfigLister, kubeClient, s)
	return s
}

// GetPlugin retrieves a CSI volume plugin by driver name, dynamically discovering it if not present.
func (s *Service) GetPlugin(ctx context.Context, driverName string) (volume.VolumePlugin, error) {
	s.mu.RLock()
	plugin, ok := s.volumePlugins[driverName]
	s.mu.RUnlock()
	if ok {
		return plugin, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if plugin, ok := s.volumePlugins[driverName]; ok {
		return plugin, nil
	}

	slog.InfoContext(ctx, "Driver not found in map, attempting dynamic discovery", slog.String("driver", driverName))

	var endpoint string
	if s.csiDriverConfigLister != nil {
		cfg, err := s.csiDriverConfigLister.Get(driverName)
		if err == nil {
			endpoint = cfg.Spec.ControllerEndpoint
			slog.InfoContext(ctx, "Found CSIDriverConfig", slog.String("driver", driverName), slog.String("endpoint", endpoint))
		} else {
			slog.WarnContext(ctx, "Failed to get CSIDriverConfig from lister", slog.String("driver", driverName), slog.Any("error", err))
		}
	}

	if endpoint == "" {
		return nil, fmt.Errorf("no CSI driver config found for %q and no default controller endpoint available", driverName)
	}

	csiClient, err := csi.NewCSIClient(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CSI client for %q: %w", driverName, err)
	}
	csiPlugin := csi.NewPlugin(csiClient)

	reportedName, err := csiPlugin.DriverName(ctx)
	if err != nil {
		csiClient.Close()
		return nil, fmt.Errorf("failed to get driver name from plugin %q: %w", driverName, err)
	}
	if reportedName != driverName {
		csiClient.Close()
		return nil, fmt.Errorf("reported driver name %q does not match requested name %q", reportedName, driverName)
	}

	s.volumePlugins[driverName] = csiPlugin
	slog.InfoContext(ctx, "Successfully registered dynamic CSI volume plugin (controller)", slog.String("driver", driverName), slog.String("endpoint", endpoint))
	return csiPlugin, nil
}
