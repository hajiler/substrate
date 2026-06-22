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

package ategcs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("ategcs")

type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

func FetchFromGCS(ctx context.Context, client ObjectStorage, gsURL string) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "fetchFromGCS")
	defer span.End()

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("while reading all content: %w", err)
	}

	return content, nil
}

// Open streams the object at gsURL; the caller must Close the returned reader.
// Unlike FetchFromGCS it does not buffer the whole object in memory.
func Open(ctx context.Context, client ObjectStorage, gsURL string) (io.ReadCloser, error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}
	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	return rc, nil
}

// SendBytesToGCS uploads the given bytes (uncompressed) to gsURL. Intended for
// small objects such as the snapshot manifest.
func SendBytesToGCS(ctx context.Context, client ObjectStorage, gsURL string, content []byte) error {
	ctx, span := tracer.Start(ctx, "sendBytesToGCS")
	defer span.End()

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}
	if err := client.PutObject(ctx, bucket, object, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("while putting object bucket=%q object=%q: %w", bucket, object, err)
	}
	return nil
}

func SendLocalFileToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "sendLocalFileToGCSWithZstd")
	defer span.End()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := sendToGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("in sendToGCSWithZstd: %w", err)
	}

	return nil
}

// streamingPutter is an ObjectStorage whose PutObject accepts a non-seekable
// streaming body without buffering (GCS). See gcsClient.SupportsStreamingPut.
type streamingPutter interface{ SupportsStreamingPut() bool }

// sendToGCSWithZstd zstd-compresses content and uploads it to gsURL.
//
// The snapshot memory-ranges is the large object here (the whole guest RAM image,
// mostly zero) on the SUSPEND critical path, so we compress with SpeedFastest across
// all CPUs — high-ratio levels scan the multi-GiB image far slower for little size
// gain on near-zero data, and the decoder auto-detects the level so restore + older
// snapshots are unaffected.
//
// Upload strategy depends on the backend:
//   - Streaming backends (GCS) accept a non-seekable body, so we pipe the compressor
//     straight into PutObject: the compress overlaps the network PUT and we never
//     stage the ~100MiB compressed payload to a temp file.
//   - S3/rustfs PutObject hands the body to the AWS SDK, which needs a seekable body
//     to sign + set Content-Length (a non-seekable pipe hangs there), so we compress
//     to a SEEKABLE temp file first.
func sendToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, content io.Reader) (err error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}
	tStart := time.Now()

	if sp, ok := client.(streamingPutter); ok && sp.SupportsStreamingPut() {
		return sendToGCSStreaming(ctx, client, bucket, object, content, tStart)
	}

	tmpFile, err := os.CreateTemp("", "substrate-upload-compress-")
	if err != nil {
		return fmt.Errorf("while creating temp compress file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	t0 := time.Now()
	var logical, dataBytes int64
	sparse := false
	if f, ok := content.(*os.File); ok {
		// Sparse-extent format: compress ONLY the populated extents (skip the holes).
		// The memory-ranges image is mostly holes (free guest RAM), so this cuts the
		// compress from scanning the whole logical image to scanning the resident set.
		// readers auto-detect this via the magic header (older plain-zstd snapshots
		// still restore). See writeSparseZstd.
		logical, dataBytes, err = writeSparseZstd(tmpFile, f)
		if err != nil {
			return fmt.Errorf("while sparse-compressing %q: %w", object, err)
		}
		sparse = true
	} else {
		// Non-file reader (no holes to exploit): plain zstd stream.
		logical, err = plainZstd(tmpFile, content)
		if err != nil {
			return fmt.Errorf("while compressing data to temp file: %w", err)
		}
		dataBytes = logical
	}
	dCompress := time.Since(t0)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("while seeking temp file: %w", err)
	}
	if err := client.PutObject(ctx, bucket, object, tmpFile); err != nil {
		return fmt.Errorf("while putting object %q: %w", object, err)
	}
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Bool("sparse", sparse),
		slog.Int64("logical", logical), slog.Int64("data", dataBytes),
		slog.Duration("compress", dCompress), slog.Duration("total", time.Since(tStart)))
	return nil
}

// sendToGCSStreaming compresses content and uploads it in one overlapped pass: a
// goroutine writes the (sparse-extent or plain) zstd stream into an io.Pipe while
// PutObject streams the read end to the object store. No seekable temp file, and
// the compress runs concurrently with the network PUT. Used only for streaming
// backends (GCS); see sendToGCSWithZstd.
func sendToGCSStreaming(ctx context.Context, client ObjectStorage, bucket, object string, content io.Reader, tStart time.Time) error {
	type result struct {
		logical, dataBytes int64
		sparse             bool
		err                error
	}
	pr, pw := io.Pipe()
	ch := make(chan result, 1)
	go func() {
		var r result
		if f, ok := content.(*os.File); ok {
			r.sparse = true
			r.logical, r.dataBytes, r.err = writeSparseZstd(pw, f)
		} else {
			r.logical, r.err = plainZstd(pw, content)
			r.dataBytes = r.logical
		}
		// Closing the writer delivers EOF (or the compress error) to PutObject.
		_ = pw.CloseWithError(r.err)
		ch <- r
	}()

	putErr := client.PutObject(ctx, bucket, object, pr)
	if putErr != nil {
		// PutObject bailed (e.g. mid-stream); unblock the compressor goroutine so it
		// can finish and we don't deadlock on the channel receive below.
		_ = pr.CloseWithError(putErr)
	}
	r := <-ch
	if putErr != nil {
		return fmt.Errorf("while putting object %q: %w", object, putErr)
	}
	if r.err != nil {
		return fmt.Errorf("while compressing %q: %w", object, r.err)
	}
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Bool("sparse", r.sparse), slog.Bool("streaming", true),
		slog.Int64("logical", r.logical), slog.Int64("data", r.dataBytes),
		slog.Duration("total", time.Since(tStart)))
	return nil
}

// plainZstd writes src to w as a single plain zstd stream (SpeedFastest, all
// cores) and returns the uncompressed byte count.
func plainZstd(w io.Writer, src io.Reader) (int64, error) {
	zw, err := zstd.NewWriter(w,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(zw, src)
	if err != nil {
		zw.Close()
		return n, err
	}
	return n, zw.Close()
}

func FetchLocalFileFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "fetchLocalFileFromGCSWithZstd")
	defer span.End()

	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := localFile.Chmod(0o600); err != nil {
		return fmt.Errorf("in localFile.Chmod(0o600): %w", err)
	}

	if err := fetchFromGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from GCS: %w", gsURL, err)
	}

	return nil
}

func fetchFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, out io.Writer) (err error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object: %w", err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			if err != nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from rc.Close", slog.Any("err", closeErr))
			}
		}
	}()

	// Peek the first bytes to pick the decode path: the sparse-extent format starts
	// with sparseMagic; anything else is a plain zstd stream (older snapshots, or the
	// non-file upload path). This keeps restore backward-compatible with snapshots
	// written before the sparse-extent format.
	magic := make([]byte, len(sparseMagic))
	n, rerr := io.ReadFull(rc, magic)
	if rerr == nil && string(magic) == sparseMagic {
		f, ok := out.(*os.File)
		if !ok {
			return fmt.Errorf("sparse-extent snapshot requires a file destination, got %T", out)
		}
		t0 := time.Now()
		size, derr := readSparseZstd(f, rc) // rc is positioned just after the magic
		if derr != nil {
			return fmt.Errorf("in sparse-extent decode: %w", derr)
		}
		slog.InfoContext(ctx, "Sparse-extent zstd download",
			slog.Int64("size", size), slog.Duration("took", time.Since(t0)))
		return nil
	}
	if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
		return fmt.Errorf("while reading object header: %w", rerr)
	}

	// Plain zstd stream: put back the bytes we peeked, then decompress. Write SPARSE
	// when the destination is a file (skip zero blocks → holes) so we only write the
	// resident set, not a dense multi-GiB image; falls back to io.Copy otherwise.
	src := io.MultiReader(bytes.NewReader(magic[:n]), rc)
	zrc, err := zstd.NewReader(src, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("in zstd.NewReader: %w", err)
	}
	defer zrc.Close()
	if f, ok := out.(*os.File); ok {
		t0 := time.Now()
		size, written, derr := copyZstdSparse(f, zrc)
		if derr != nil {
			return fmt.Errorf("in sparse decompress: %w", derr)
		}
		slog.InfoContext(ctx, "Sparse zstd download (plain)",
			slog.Int64("size", size), slog.Int64("written", written), slog.Duration("took", time.Since(t0)))
		return nil
	}
	if _, err = io.Copy(out, zrc); err != nil {
		return fmt.Errorf("in io.Copy: %w", err)
	}

	return nil
}

// copyZstdSparse copies src into dst skipping all-zero blocks, so dst becomes a
// sparse file (the skipped regions are holes). Returns the logical size (total bytes
// consumed from src) and the bytes actually written (non-zero). dst is truncated to
// the logical size at the end so trailing zero regions become a hole and the file
// size is exact. dst must be a fresh/truncated regular file opened for writing.
func copyZstdSparse(dst *os.File, src io.Reader) (size int64, written int64, err error) {
	// 64KiB blocks: a multiple of the 4KiB fs block (so skipped runs align to whole
	// hole-able blocks) while keeping the zero-scan + WriteAt syscall count modest.
	const block = 64 << 10
	buf := make([]byte, block)
	var pos int64
	for {
		n, rerr := io.ReadFull(src, buf)
		if n > 0 {
			chunk := buf[:n]
			if !allZero(chunk) {
				if _, werr := dst.WriteAt(chunk, pos); werr != nil {
					return 0, 0, werr
				}
				written += int64(n)
			}
			pos += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return 0, 0, rerr
		}
	}
	// Materialize the exact logical size: extends past the last written byte with a
	// hole when the tail was zero (skipped), and is a no-op otherwise.
	if terr := dst.Truncate(pos); terr != nil {
		return 0, 0, terr
	}
	return pos, written, nil
}

// allZero reports whether b is all zero bytes, checking 8 bytes at a time.
func allZero(b []byte) bool {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if binary.LittleEndian.Uint64(b[i:]) != 0 {
			return false
		}
	}
	for ; i < len(b); i++ {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func parseGCSURL(gsURL string) (string, string, error) {
	parsed, err := url.Parse(gsURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", gsURL, err)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}
