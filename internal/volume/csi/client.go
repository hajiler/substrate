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

package csi

import (
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Connect connects to the CSI endpoint (Unix domain socket).
// The endpoint should be in the format "unix:///path/to/socket" or "/path/to/socket".
func Connect(endpoint string) (*grpc.ClientConn, error) {
	proto, addr, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	if proto != "unix" {
		return nil, fmt.Errorf("only unix domain sockets are supported, got %q", proto)
	}

	target := fmt.Sprintf("unix://%s", addr)
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CSI socket %q: %w", addr, err)
	}

	return conn, nil
}

func parseEndpoint(endpoint string) (string, string, error) {
	if !strings.HasPrefix(endpoint, "unix://") && !strings.HasPrefix(endpoint, "tcp://") {
		// Assume unix if no scheme
		return "unix", endpoint, nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", err
	}

	return u.Scheme, u.Path, nil
}
