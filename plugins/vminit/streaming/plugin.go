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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/streaming"
	"github.com/containerd/containerd/v2/pkg/shutdown"
	cplugins "github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
	"github.com/containerd/typeurl/v2"
	"github.com/mdlayher/vsock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/containerd/nerdbox/plugins"
)

type serviceConfig struct {
	ContextID uint32
	Port      uint32
}

func (config *serviceConfig) SetVsock(cid, port uint32) {
	config.ContextID = cid
	config.Port = port
}

func init() {
	registry.Register(&plugin.Registration{
		Type: plugins.StreamingPlugin,
		ID:   "vsock",
		Requires: []plugin.Type{
			cplugins.InternalPlugin,
		},
		Config: &serviceConfig{},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ss, err := ic.GetByID(cplugins.InternalPlugin, "shutdown")
			if err != nil {
				return nil, err
			}
			config := ic.Config.(*serviceConfig)
			l, err := vsock.ListenContextID(config.ContextID, config.Port, &vsock.Config{})
			if err != nil {
				return nil, fmt.Errorf("failed to listen on vsock port %d with context id %d: %w", config.Port, config.ContextID, err)
			}
			// Diag (sandboxes#2529): vsock streaming listener is up. This is
			// the listener that, when closed, causes the kernel to emit
			// op=Shutdown src=1026 dst=1024 packets seen on the host.
			log.L.WithFields(map[string]any{
				"port":       config.Port,
				"context_id": config.ContextID,
				"local":      l.Addr().String(),
			}).Info("diag-vsock-listener-open")

			s := &service{
				l:       l,
				streams: make(map[string]net.Conn),
			}

			ss.(shutdown.Service).RegisterCallback(s.Shutdown)

			go s.Run()

			return s, nil
		},
	})
}

type service struct {
	mu sync.Mutex
	l  net.Listener

	streams map[string]net.Conn
}

func (s *service) Shutdown(ctx context.Context) error {
	// Diag (sandboxes#2529): vminitd is closing the vsock 1026 listener.
	// This is the prime suspect for the op=Shutdown packets observed.
	// Capture the stack so we know which shutdown.Service path triggered us.
	log.G(ctx).WithFields(log.Fields{
		"port":         1026,
		"open_streams": len(s.streams),
		"stack":        string(debug.Stack()),
	}).Info("diag-vsock-listener-closed")

	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error

	// Close all connections
	for _, conn := range s.streams {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}

	if s.l != nil {
		if err := s.l.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close listener: %w", err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *service) Run() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			// Diag (sandboxes#2529): listener accept loop exiting; record
			// the underlying error so we can tell whether it was an explicit
			// Close(), a kernel-level reset, or something else.
			log.L.WithError(err).WithFields(map[string]any{
				"port":     1026,
				"err_type": fmt.Sprintf("%T", err),
			}).Info("diag-vsock-listener-accept-exit")
			return // Listener closed
		}
		acceptedAt := time.Now()
		// Diag (sandboxes#2529): per-connection accepted log on the
		// vminitd-side listener. Wrap the conn so its Close() is logged too.
		log.L.WithFields(map[string]any{
			"port":   1026,
			"remote": conn.RemoteAddr().String(),
			"local":  conn.LocalAddr().String(),
		}).Info("diag-vsock-stream-accepted")
		conn = &diagAcceptedConn{Conn: conn, openedAt: acceptedAt}

		// Read length-prefixed stream ID
		var idLen uint32
		if err := binary.Read(conn, binary.BigEndian, &idLen); err != nil {
			log.L.WithError(err).Debug("failed to read stream ID length")
			conn.Close()
			continue
		}
		idBytes := make([]byte, idLen)
		if _, err := io.ReadFull(conn, idBytes); err != nil {
			log.L.WithError(err).Debug("failed to read stream ID")
			conn.Close()
			continue
		}
		streamID := string(idBytes)
		if dac, ok := conn.(*diagAcceptedConn); ok {
			dac.streamID = streamID
		}

		s.mu.Lock()
		if _, ok := s.streams[streamID]; ok {
			s.mu.Unlock()
			log.L.WithField("stream", streamID).Debug("duplicate stream ID, rejecting")
			// Send back an error message so the client gets a meaningful rejection
			errMsg := fmt.Sprintf("stream %q already exists", streamID)
			writeString(conn, errMsg)
			conn.Close()
			continue
		}
		s.streams[streamID] = conn
		s.mu.Unlock()

		// Ack: echo back the stream ID
		if err := writeString(conn, streamID); err != nil {
			s.removeStream(streamID)
			conn.Close()
			continue
		}
	}
}

// diagAcceptedConn wraps a vminitd-side accepted vsock conn so we can log
// when each per-stream connection closes. Diag (sandboxes#2529): combined
// with diag-vsock-stream-accepted, this lets us correlate stream lifetime
// with the op=Shutdown packets observed by the host.
type diagAcceptedConn struct {
	net.Conn
	streamID string
	openedAt time.Time
	once     sync.Once
}

func (c *diagAcceptedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		log.L.WithFields(map[string]any{
			"port":        1026,
			"stream":      c.streamID,
			"remote":      c.Conn.RemoteAddr().String(),
			"duration_ms": time.Since(c.openedAt).Milliseconds(),
		}).Info("diag-vsock-stream-closed")
	})
	return err
}

func (s *service) removeStream(streamID string) {
	s.mu.Lock()
	delete(s.streams, streamID)
	s.mu.Unlock()
}

// writeString writes a length-prefixed string to the connection.
func writeString(conn net.Conn, s string) error {
	b := []byte(s)
	if err := binary.Write(conn, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := conn.Write(b)
	return err
}

// Get returns the raw connection for the given stream ID, removing it from
// the map. This implements stream.Manager for the task service IO forwarding.
func (s *service) Get(id string) (io.ReadWriteCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn, ok := s.streams[id]
	if !ok {
		return nil, fmt.Errorf("stream %q not found: %w", id, errdefs.ErrNotFound)
	}
	delete(s.streams, id)
	return conn, nil
}

// StreamGetter returns a streaming.StreamGetter that looks up streams by
// their string stream ID.
func (s *service) StreamGetter() streaming.StreamGetter {
	return &streamGetter{s: s}
}

type streamGetter struct {
	s *service
}

func (sg *streamGetter) Get(ctx context.Context, name string) (streaming.Stream, error) {
	sg.s.mu.Lock()
	conn, ok := sg.s.streams[name]
	if !ok {
		sg.s.mu.Unlock()
		return nil, fmt.Errorf("stream %q not found: %w", name, errdefs.ErrNotFound)
	}
	// Remove from map so the stream is exclusively owned by the caller.
	// The caller is responsible for closing the stream.
	delete(sg.s.streams, name)
	sg.s.mu.Unlock()
	return &vsockStream{conn: conn}, nil
}

// maxFrameSize is the maximum allowed frame payload (10 MiB). Frames
// larger than this are rejected to prevent OOM from buggy/malicious peers.
const maxFrameSize = 10 << 20

// vsockStream wraps a net.Conn with length-prefixed proto framing to
// implement the streaming.Stream interface. Each message is framed as
// a 4-byte big-endian length prefix followed by serialized proto bytes.
type vsockStream struct {
	conn net.Conn
	once sync.Once // ensures Close sends EOF exactly once
}

func (s *vsockStream) Send(a typeurl.Any) error {
	data, err := proto.Marshal(typeurl.MarshalProto(a))
	if err != nil {
		return fmt.Errorf("failed to marshal stream message: %w", err)
	}
	if err := binary.Write(s.conn, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("failed to write frame length: %w", err)
	}
	if _, err := s.conn.Write(data); err != nil {
		return fmt.Errorf("failed to write frame data: %w", err)
	}
	return nil
}

func (s *vsockStream) Recv() (typeurl.Any, error) {
	var length uint32
	if err := binary.Read(s.conn, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	// A zero-length frame is an application-level EOF marker.
	if length == 0 {
		return nil, io.EOF
	}
	if length > maxFrameSize {
		return nil, fmt.Errorf("frame size %d exceeds maximum %d", length, maxFrameSize)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(s.conn, data); err != nil {
		return nil, fmt.Errorf("failed to read frame data: %w", err)
	}
	var a anypb.Any
	if err := proto.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stream message: %w", err)
	}
	return &a, nil
}

func (s *vsockStream) Close() error {
	var err error
	s.once.Do(func() {
		// Send a zero-length frame as an application-level EOF marker.
		// Do NOT close the underlying connection — the vsock proxy sends
		// a bidirectional SHUTDOWN on transport close, which can race with
		// in-flight data packets and cause the peer to lose the last chunk.
		// The connection is cleaned up when the VM shuts down.
		err = binary.Write(s.conn, binary.BigEndian, uint32(0))
	})
	return err
}
