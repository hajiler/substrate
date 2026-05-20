//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// Command counter-volume is a simple server that will be used as a worker pod. It listens on ports 80
// and returns a greeting with the IP of the pod where it is running, and writes a test file on a mounted volume.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
)

var requestCount uint64

func main() {
	flag.Parse()
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	defaultMux := http.NewServeMux()
	defaultMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		count := atomic.AddUint64(&requestCount, 1)

		var fileVal string
		fileVal = "test"

		currentIP := getCurrentIP()
		response := fmt.Sprintf("hello from: %s | preserved memory count: %d | file content: %s\n", currentIP, count, fileVal)
		slog.InfoContext(ctx, "Handled request", slog.String("response", response))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})

	go func() {
		slog.InfoContext(ctx, "Starting server on port 80")
		if err := http.ListenAndServe(":80", defaultMux); err != nil {
			slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
			os.Exit(1)
		}
	}()
}

func updateCountFile() error {
	count := atomic.LoadUint64(&requestCount)
	rf, err := os.Create("/data/random-content-file")
	if err != nil {
		return fmt.Errorf("while opening file: %w", err)
	}
	defer rf.Close()

	_, err = fmt.Fprintf(rf, "%d", count)
	if err != nil {
		return fmt.Errorf("while writing count: %w", err)
	}

	return nil
}

func readCountFile() (error, string) {
	fileContent, err := os.ReadFile("/data/random-content-file")
	if err != nil {
		slog.Error("Error reading count file", slog.Any("err", err))
		return err, ""
	}
	return nil, string(fileContent)
}

func getCurrentIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		slog.Error("Error getting interface addresses", slog.Any("err", err))
		return "x.x.x.x"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "y.y.y.y"
}
