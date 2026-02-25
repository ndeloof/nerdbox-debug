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

package streaming

import (
	"context"
	"sync"

	"github.com/containerd/containerd/v2/core/streaming"
	cplugins "github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
)

func init() {
	registry.Register(&plugin.Registration{
		Type: cplugins.StreamingPlugin,
		ID:   "manager",
		InitFn: func(ic *plugin.InitContext) (any, error) {
			return &streamManager{
				streams: make(map[string]*managedStream),
			}, nil
		},
	})
}

type streamManager struct {
	mu      sync.Mutex
	streams map[string]*managedStream
}

func (sm *streamManager) Register(ctx context.Context, name string, stream streaming.Stream) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.streams[name]; ok {
		return errdefs.ErrAlreadyExists
	}
	sm.streams[name] = &managedStream{
		Stream:  stream,
		name:    name,
		manager: sm,
	}
	return nil
}

func (sm *streamManager) Get(ctx context.Context, name string) (streaming.Stream, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stream, ok := sm.streams[name]
	if !ok {
		return nil, errdefs.ErrNotFound
	}
	return stream, nil
}

type managedStream struct {
	streaming.Stream

	name    string
	manager *streamManager
}

func (m *managedStream) Close() error {
	m.manager.mu.Lock()
	delete(m.manager.streams, m.name)
	m.manager.mu.Unlock()
	return m.Stream.Close()
}
