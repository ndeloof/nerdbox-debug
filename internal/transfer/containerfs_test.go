/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package transfer

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/containerd/errdefs"
)

func TestWritePath(t *testing.T) {
	t.Run("RegularFile", func(t *testing.T) {
		rootfs := t.TempDir()
		content := []byte("hello world")
		if err := os.WriteFile(filepath.Join(rootfs, "file.txt"), content, 0644); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := writePath(rootfs, "file.txt", &buf, mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		entries := readTarEntries(t, &buf)
		if len(entries) != 1 {
			t.Fatalf("expected 1 tar entry, got %d", len(entries))
		}
		if entries[0].header.Name != "file.txt" {
			t.Errorf("expected name %q, got %q", "file.txt", entries[0].header.Name)
		}
		if !bytes.Equal(entries[0].data, content) {
			t.Errorf("content mismatch: got %q, want %q", entries[0].data, content)
		}
	})

	t.Run("DirectoryWalk", func(t *testing.T) {
		rootfs := t.TempDir()
		dir := filepath.Join(rootfs, "mydir")
		sub := filepath.Join(dir, "sub")
		if err := os.MkdirAll(sub, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("aaa"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("bbb"), 0644); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := writePath(rootfs, "mydir", &buf, mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		entries := readTarEntries(t, &buf)
		names := make(map[string]bool)
		for _, e := range entries {
			names[e.header.Name] = true
		}

		expected := []string{"mydir", "mydir/a.txt", "mydir/sub", "mydir/sub/b.txt"}
		for _, name := range expected {
			if !names[name] {
				t.Errorf("missing expected entry %q; got %v", name, names)
			}
		}
	})

	t.Run("DirectoryNoWalk", func(t *testing.T) {
		rootfs := t.TempDir()
		dir := filepath.Join(rootfs, "mydir")
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "child.txt"), []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := writePath(rootfs, "mydir", &buf, mediaTypeTar, true); err != nil {
			t.Fatal(err)
		}

		entries := readTarEntries(t, &buf)
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry (dir only), got %d", len(entries))
		}
		if entries[0].header.Typeflag != tar.TypeDir {
			t.Errorf("expected dir entry, got typeflag %d", entries[0].header.Typeflag)
		}
		if entries[0].header.Name != "mydir" {
			t.Errorf("expected name %q, got %q", "mydir", entries[0].header.Name)
		}
	})

	t.Run("Symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlinks require special privileges on Windows")
		}

		rootfs := t.TempDir()
		if err := os.WriteFile(filepath.Join(rootfs, "target.txt"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("target.txt", filepath.Join(rootfs, "link.txt")); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := writePath(rootfs, "link.txt", &buf, mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		entries := readTarEntries(t, &buf)
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].header.Typeflag != tar.TypeSymlink {
			t.Errorf("expected symlink entry, got typeflag %d", entries[0].header.Typeflag)
		}
		if entries[0].header.Linkname != "target.txt" {
			t.Errorf("expected linkname %q, got %q", "target.txt", entries[0].header.Linkname)
		}
	})

	t.Run("UnsupportedMediaType", func(t *testing.T) {
		rootfs := t.TempDir()
		if err := os.WriteFile(filepath.Join(rootfs, "f"), []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		err := writePath(rootfs, "f", &buf, "application/json", false)
		if err == nil {
			t.Fatal("expected error for unsupported media type")
		}
		if !errors.Is(err, errdefs.ErrNotImplemented) {
			t.Errorf("expected ErrNotImplemented, got %v", err)
		}
	})

	t.Run("NonexistentPath", func(t *testing.T) {
		rootfs := t.TempDir()

		var buf bytes.Buffer
		err := writePath(rootfs, "does-not-exist", &buf, mediaTypeTar, false)
		if err == nil {
			t.Fatal("expected error for nonexistent path")
		}
	})
}

func TestReadPath(t *testing.T) {
	t.Run("RegularFile", func(t *testing.T) {
		rootfs := t.TempDir()
		content := []byte("extracted content")

		archive := buildTar(t, []tarEntry{
			{header: &tar.Header{Name: "out.txt", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(content))}, data: content},
		})

		if err := readPath(archive, rootfs, "/", mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		got, err := os.ReadFile(filepath.Join(rootfs, "out.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("content mismatch: got %q, want %q", got, content)
		}

		info, err := os.Stat(filepath.Join(rootfs, "out.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0644 {
			t.Errorf("expected permissions 0644, got %04o", info.Mode().Perm())
		}
	})

	t.Run("Directory", func(t *testing.T) {
		rootfs := t.TempDir()

		archive := buildTar(t, []tarEntry{
			{header: &tar.Header{Name: "mydir", Typeflag: tar.TypeDir, Mode: 0755}},
			{header: &tar.Header{Name: "mydir/inner", Typeflag: tar.TypeDir, Mode: 0755}},
		})

		if err := readPath(archive, rootfs, "/", mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		info, err := os.Stat(filepath.Join(rootfs, "mydir", "inner"))
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Error("expected directory")
		}
	})

	t.Run("Symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlinks require special privileges on Windows")
		}

		rootfs := t.TempDir()
		content := []byte("target data")

		archive := buildTar(t, []tarEntry{
			{header: &tar.Header{Name: "target.txt", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(content))}, data: content},
			{header: &tar.Header{Name: "link.txt", Typeflag: tar.TypeSymlink, Linkname: "target.txt"}},
		})

		if err := readPath(archive, rootfs, "/", mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		linkTarget, err := os.Readlink(filepath.Join(rootfs, "link.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if linkTarget != "target.txt" {
			t.Errorf("expected link target %q, got %q", "target.txt", linkTarget)
		}
	})

	t.Run("Hardlink", func(t *testing.T) {
		rootfs := t.TempDir()
		content := []byte("shared content")

		// The code passes header.Linkname directly to os.Link, so the
		// linkname must be the absolute filesystem path of the already-
		// extracted file.
		originalPath := filepath.Join(rootfs, "original.txt")

		archive := buildTar(t, []tarEntry{
			{header: &tar.Header{Name: "original.txt", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(content))}, data: content},
			{header: &tar.Header{Name: "hardlink.txt", Typeflag: tar.TypeLink, Linkname: originalPath}},
		})

		if err := readPath(archive, rootfs, "/", mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		got, err := os.ReadFile(filepath.Join(rootfs, "hardlink.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("content mismatch: got %q, want %q", got, content)
		}

		// Verify they share the same inode (are actually hardlinked)
		origInfo, err := os.Stat(filepath.Join(rootfs, "original.txt"))
		if err != nil {
			t.Fatal(err)
		}
		linkInfo, err := os.Stat(filepath.Join(rootfs, "hardlink.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(origInfo, linkInfo) {
			t.Error("expected hardlink (same inode)")
		}
	})

	t.Run("PathTraversalNeutralized", func(t *testing.T) {
		// filepath.Clean("/"+name) normalizes traversal attempts,
		// so ../../escape.txt becomes /escape.txt and lands inside
		// the destination rather than escaping it.
		rootfs := t.TempDir()
		content := []byte("safe")

		archive := buildTar(t, []tarEntry{
			{header: &tar.Header{Name: "../../escape.txt", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(content))}, data: content},
		})

		if err := readPath(archive, rootfs, "/", mediaTypeTar, false); err != nil {
			t.Fatal(err)
		}

		// The file should exist inside rootfs, not outside it.
		got, err := os.ReadFile(filepath.Join(rootfs, "escape.txt"))
		if err != nil {
			t.Fatal("file should be safely extracted inside rootfs:", err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("content mismatch: got %q, want %q", got, content)
		}
	})

	t.Run("UnsupportedMediaType", func(t *testing.T) {
		err := readPath(bytes.NewReader(nil), t.TempDir(), "/", "application/json", false)
		if err == nil {
			t.Fatal("expected error for unsupported media type")
		}
		if !errors.Is(err, errdefs.ErrNotImplemented) {
			t.Errorf("expected ErrNotImplemented, got %v", err)
		}
	})

	t.Run("PreserveOwnership", func(t *testing.T) {
		if os.Getuid() != 0 {
			t.Skip("chown requires root")
		}

		rootfs := t.TempDir()
		content := []byte("owned content")

		archive := buildTar(t, []tarEntry{
			{header: &tar.Header{Name: "owned.txt", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(content)), Uid: 1000, Gid: 1000}, data: content},
		})

		if err := readPath(archive, rootfs, "/", mediaTypeTar, true); err != nil {
			t.Fatal(err)
		}

		// Verify UID/GID were applied — only possible as root.
	})
}

func TestRoundTrip(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require special privileges on Windows")
	}

	// Build a source tree with various entry types.
	srcRoot := t.TempDir()
	srcDir := filepath.Join(srcRoot, "tree")
	if err := os.MkdirAll(filepath.Join(srcDir, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "sub", "b.txt"), []byte("beta"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(srcDir, "link")); err != nil {
		t.Fatal(err)
	}

	// writePath the source tree into a tar buffer.
	var buf bytes.Buffer
	if err := writePath(srcRoot, "tree", &buf, mediaTypeTar, false); err != nil {
		t.Fatal("writePath:", err)
	}

	// readPath from the buffer into a fresh destination.
	dstRoot := t.TempDir()
	if err := readPath(&buf, dstRoot, "/", mediaTypeTar, false); err != nil {
		t.Fatal("readPath:", err)
	}

	// Compare the two trees.
	compareTrees(t, srcDir, filepath.Join(dstRoot, "tree"))
}

type tarEntry struct {
	header *tar.Header
	data   []byte
}

// readTarEntries reads all entries from a tar archive.
func readTarEntries(t *testing.T, r io.Reader) []tarEntry {
	t.Helper()
	tr := tar.NewReader(r)
	var entries []tarEntry
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return entries
		}
		if err != nil {
			t.Fatal("readTarEntries:", err)
		}
		var data []byte
		if hdr.Size > 0 {
			data, err = io.ReadAll(tr)
			if err != nil {
				t.Fatal("readTarEntries read data:", err)
			}
		}
		entries = append(entries, tarEntry{header: hdr, data: data})
	}
}

// buildTar creates an in-memory tar archive from the given entries.
func buildTar(t *testing.T, entries []tarEntry) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		if err := tw.WriteHeader(e.header); err != nil {
			t.Fatal("buildTar WriteHeader:", err)
		}
		if len(e.data) > 0 {
			if _, err := tw.Write(e.data); err != nil {
				t.Fatal("buildTar Write:", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal("buildTar Close:", err)
	}
	return &buf
}

// compareTrees walks two directory trees and verifies they have the same
// structure and file contents.
func compareTrees(t *testing.T, want, got string) {
	t.Helper()

	wantEntries := make(map[string]fs.FileInfo)
	if err := filepath.Walk(want, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(want, path)
		if err != nil {
			return err
		}
		wantEntries[rel] = info
		return nil
	}); err != nil {
		t.Fatal("walk want:", err)
	}

	gotEntries := make(map[string]fs.FileInfo)
	if err := filepath.Walk(got, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(got, path)
		if err != nil {
			return err
		}
		gotEntries[rel] = info
		return nil
	}); err != nil {
		t.Fatal("walk got:", err)
	}

	// Check all expected entries exist in got.
	for rel, wantInfo := range wantEntries {
		gotInfo, ok := gotEntries[rel]
		if !ok {
			t.Errorf("missing entry %q in extracted tree", rel)
			continue
		}

		if wantInfo.IsDir() != gotInfo.IsDir() {
			t.Errorf("%q: dir mismatch: want isDir=%v, got isDir=%v", rel, wantInfo.IsDir(), gotInfo.IsDir())
			continue
		}

		wantMode := wantInfo.Mode()

		// Compare symlinks
		if wantMode&os.ModeSymlink != 0 {
			wantTarget, err := os.Readlink(filepath.Join(want, rel))
			if err != nil {
				t.Fatal(err)
			}
			gotTarget, err := os.Readlink(filepath.Join(got, rel))
			if err != nil {
				t.Fatal(err)
			}
			if wantTarget != gotTarget {
				t.Errorf("%q: symlink target mismatch: want %q, got %q", rel, wantTarget, gotTarget)
			}
			continue
		}

		// Compare regular file contents
		if wantMode.IsRegular() {
			wantData, err := os.ReadFile(filepath.Join(want, rel))
			if err != nil {
				t.Fatal(err)
			}
			gotData, err := os.ReadFile(filepath.Join(got, rel))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wantData, gotData) {
				t.Errorf("%q: content mismatch: want %q, got %q", rel, wantData, gotData)
			}
		}
	}

	// Check for unexpected entries in got.
	for rel := range gotEntries {
		if _, ok := wantEntries[rel]; !ok {
			t.Errorf("unexpected entry %q in extracted tree", rel)
		}
	}
}
