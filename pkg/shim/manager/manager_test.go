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

package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewName(t *testing.T) {
	const name = "io.containerd.nerdbox.v1"
	s := New(name)
	if got := s.Name(); got != name {
		t.Errorf("Name() = %q, want %q", got, name)
	}
}

func TestInfo(t *testing.T) {
	const name = "io.containerd.nerdbox.v1"
	s := New(name)
	info, err := s.Info(context.Background(), strings.NewReader(""))
	if err != nil {
		t.Fatalf("Info() error: %v", err)
	}
	if info.Name != name {
		t.Errorf("Info().Name = %q, want %q", info.Name, name)
	}
	if got := info.Annotations["containerd.io/runtime-allow-mounts"]; got == "" {
		t.Errorf("Info().Annotations missing runtime-allow-mounts: %v", info.Annotations)
	}
}

func TestReadSpec(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	const body = `{"annotations":{"io.kubernetes.cri.sandbox-id":"abc123","other":"value"}}`
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(cwd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	spec, err := readSpec()
	if err != nil {
		t.Fatalf("readSpec() error: %v", err)
	}
	if got := spec.Annotations["io.kubernetes.cri.sandbox-id"]; got != "abc123" {
		t.Errorf("annotation sandbox-id = %q, want %q", got, "abc123")
	}
	if got := spec.Annotations["other"]; got != "value" {
		t.Errorf("annotation other = %q, want %q", got, "value")
	}
}

func TestReadSpecMissing(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(cwd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	if _, err := readSpec(); !os.IsNotExist(err) {
		t.Errorf("readSpec() error = %v, want os.IsNotExist", err)
	}
}
