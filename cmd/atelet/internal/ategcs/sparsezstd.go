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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

// sparseMagic marks the sparse-extent snapshot format (see writeSparseZstd). It is
// 8 bytes and deliberately NOT a valid zstd frame magic, so a reader can tell this
// format from a plain zstd stream (no magic) by the first 8 bytes. The magic is
// version-neutral; the format version follows it (see sparseVersion).
const sparseMagic = "ATESPRSE"

// sparseVersion is the sparse-extent format version, written as a little-endian
// uint32 immediately after sparseMagic. Bump it on any incompatible layout change
// so readers reject snapshots they don't understand instead of misparsing them.
const sparseVersion uint32 = 1

// extent is a populated (non-hole) byte range of a sparse file.
type extent struct {
	off    int64
	length int64
}

// sparseExtents returns the populated regions of f (via SEEK_DATA/SEEK_HOLE) and
// f's logical size. Holes are the gaps between extents. It leaves f's offset
// undefined (callers seek explicitly before reading).
func sparseExtents(f *os.File) ([]extent, int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := fi.Size()
	fd := int(f.Fd())
	var exts []extent
	off := int64(0)
	for off < size {
		ds, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if err == unix.ENXIO { // no more data: rest is a hole
				break
			}
			return nil, 0, fmt.Errorf("SEEK_DATA: %w", err)
		}
		de, err := unix.Seek(fd, ds, unix.SEEK_HOLE)
		if err != nil {
			return nil, 0, fmt.Errorf("SEEK_HOLE: %w", err)
		}
		exts = append(exts, extent{off: ds, length: de - ds})
		off = de
	}
	return exts, size, nil
}

// writeSparseZstd encodes a sparse file src to dst in the sparse-extent format:
//
//	magic[8] | version:u32 | totalSize:i64 | numExtents:i64 | (off:i64,len:i64)*numExtents | zstd(data...)
//
// where the trailing zstd stream is the concatenation of ONLY the populated extents
// (the holes are not read or compressed). This is the upload mirror of the sparse
// DOWNLOAD: a guest memory-ranges image is mostly holes (free RAM), so feeding only
// the real extents to zstd cuts the compress from "scan the whole logical image"
// (e.g. 2GiB) to "scan the resident set" (e.g. ~150MiB). Returns the logical size
// and the data (pre-compression) bytes. The integers are little-endian.
func writeSparseZstd(dst io.Writer, src *os.File) (logical, dataBytes int64, err error) {
	exts, size, err := sparseExtents(src)
	if err != nil {
		return 0, 0, err
	}
	// Buffer the (small) header so the few fixed-size writes don't each hit dst.
	bw := bufio.NewWriter(dst)
	if _, err := bw.WriteString(sparseMagic); err != nil {
		return 0, 0, err
	}
	if err := binary.Write(bw, binary.LittleEndian, sparseVersion); err != nil {
		return 0, 0, err
	}
	for _, v := range []int64{size, int64(len(exts))} {
		if err := binary.Write(bw, binary.LittleEndian, v); err != nil {
			return 0, 0, err
		}
	}
	for _, e := range exts {
		if err := binary.Write(bw, binary.LittleEndian, e.off); err != nil {
			return 0, 0, err
		}
		if err := binary.Write(bw, binary.LittleEndian, e.length); err != nil {
			return 0, 0, err
		}
	}
	if err := bw.Flush(); err != nil {
		return 0, 0, err
	}

	zw, err := zstd.NewWriter(dst,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return 0, 0, err
	}
	for _, e := range exts {
		if _, err := src.Seek(e.off, io.SeekStart); err != nil {
			zw.Close()
			return 0, 0, err
		}
		n, err := io.CopyN(zw, src, e.length)
		dataBytes += n
		if err != nil {
			zw.Close()
			return 0, 0, fmt.Errorf("reading extent @%d+%d: %w", e.off, e.length, err)
		}
	}
	if err := zw.Close(); err != nil {
		return 0, 0, err
	}
	return size, dataBytes, nil
}

// readSparseZstd decodes the sparse-extent format into dst, which becomes a sparse
// file (the holes between extents are never written). src must be positioned just
// AFTER the magic (the caller reads + dispatches on it). dst is truncated to the
// logical size so trailing holes + the exact size are represented.
func readSparseZstd(dst *os.File, src io.Reader) (logical int64, err error) {
	var ver uint32
	if err := binary.Read(src, binary.LittleEndian, &ver); err != nil {
		return 0, fmt.Errorf("reading sparse format version: %w", err)
	}
	if ver != sparseVersion {
		return 0, fmt.Errorf("unsupported sparse snapshot format version %d (this build supports %d)", ver, sparseVersion)
	}
	var size, numExt int64
	if err := binary.Read(src, binary.LittleEndian, &size); err != nil {
		return 0, fmt.Errorf("reading totalSize: %w", err)
	}
	if err := binary.Read(src, binary.LittleEndian, &numExt); err != nil {
		return 0, fmt.Errorf("reading numExtents: %w", err)
	}
	if numExt < 0 || numExt > 1<<28 {
		return 0, fmt.Errorf("implausible numExtents %d", numExt)
	}
	exts := make([]extent, numExt)
	for i := range exts {
		if err := binary.Read(src, binary.LittleEndian, &exts[i].off); err != nil {
			return 0, err
		}
		if err := binary.Read(src, binary.LittleEndian, &exts[i].length); err != nil {
			return 0, err
		}
	}
	if err := dst.Truncate(size); err != nil {
		return 0, err
	}
	zr, err := zstd.NewReader(src, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return 0, err
	}
	defer zr.Close()
	// The zstd stream is the extents' data concatenated in order; slice it back out
	// by length and write each at its offset (the gaps stay holes).
	for _, e := range exts {
		if _, err := dst.Seek(e.off, io.SeekStart); err != nil {
			return 0, err
		}
		if _, err := io.CopyN(dst, zr, e.length); err != nil {
			return 0, fmt.Errorf("writing extent @%d+%d: %w", e.off, e.length, err)
		}
	}
	return size, nil
}
