// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"fmt"
	"sync"
)

var (
	pluginsMu sync.RWMutex
	plugins   = make(map[string]VolumePlugin)
)

// RegisterPlugin registers a volume plugin.
func RegisterPlugin(name string, plugin VolumePlugin) {
	pluginsMu.Lock()
	defer pluginsMu.Unlock()
	if plugin == nil {
		panic("Registry: Register plugin is nil")
	}
	if _, dup := plugins[name]; dup {
		panic(fmt.Sprintf("Registry: Register called twice for plugin %s", name))
	}
	plugins[name] = plugin
}

// GetPlugin retrieves a registered volume plugin.
func GetPlugin(name string) (VolumePlugin, error) {
	pluginsMu.RLock()
	defer pluginsMu.RUnlock()
	plugin, ok := plugins[name]
	if !ok {
		return nil, fmt.Errorf("volume plugin %q not found", name)
	}
	return plugin, nil
}
