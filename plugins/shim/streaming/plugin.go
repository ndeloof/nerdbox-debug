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
	"errors"
	"fmt"
	"io"

	streamapi "github.com/containerd/containerd/api/services/streaming/v1"
	cplugins "github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
	"github.com/containerd/ttrpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/containerd/nerdbox/plugins"
)

func init() {
	registry.Register(&plugin.Registration{
		Type: cplugins.TTRPCPlugin,
		ID:   "streaming",
		Requires: []plugin.Type{
			plugins.SandboxPlugin,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			sb, err := ic.GetSingle(plugins.SandboxPlugin)
			if err != nil {
				return nil, err
			}

			return &service{
				sb: sb.(sandboxClient),
			}, nil
		},
	})
}

type sandboxClient interface {
	Client() (*ttrpc.Client, error)
}

type service struct {
	sb sandboxClient
}

func (s *service) RegisterTTRPC(server *ttrpc.Server) error {
	streamapi.RegisterTTRPCStreamingService(server, s)
	return nil
}

func (s *service) Stream(ctx context.Context, srv streamapi.TTRPCStreaming_StreamServer) error {
	// Receive the StreamInit message with the stream ID
	a, err := srv.Recv()
	if err != nil {
		return err
	}

	log.G(ctx).Debug("opening TTRPC stream proxy to sandbox")

	// Open a TTRPC stream to the sandbox
	client, err := s.sb.Client()
	if err != nil {
		return fmt.Errorf("failed to get sandbox client: %w", err)
	}
	sbStream, err := streamapi.NewTTRPCStreamingClient(client).Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open sandbox stream: %w", err)
	}
	defer sbStream.CloseSend()

	// Forward StreamInit to sandbox
	if err := sbStream.Send(a); err != nil {
		return fmt.Errorf("failed to forward StreamInit to sandbox: %w", err)
	}

	// Receive ack from sandbox
	ack, err := sbStream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive ack from sandbox: %w", err)
	}

	// Forward ack to client
	if err := srv.Send(ack); err != nil {
		return err
	}

	// Proxy messages in both directions
	clientToSandbox := proxyMessages(srv, sbStream)
	sandboxToClient := proxyMessages(sbStream, srv)

	// Wait for either direction to finish or context cancellation.
	//
	// When the client ends first (normal for ReadStream/import), close the
	// send side to the sandbox so it sees EOF, then wait for the sandbox
	// to finish sending remaining messages (e.g. WindowUpdates) before
	// returning. This prevents the handler from tearing down the server
	// stream while the sandbox-to-client goroutine is still active.
	//
	// When the sandbox ends first (normal for WriteStream/export), the
	// handler returns which closes the server stream. The client will
	// see EOF on its receive side.
	select {
	case err := <-clientToSandbox:
		if err != nil && !errors.Is(err, io.EOF) {
			log.G(ctx).WithError(err).Debug("stream proxy client->sandbox ended")
		}
		// Signal sandbox that client is done, then wait for sandbox
		// direction to drain before returning.
		sbStream.CloseSend()
		if err := <-sandboxToClient; err != nil && !errors.Is(err, io.EOF) {
			log.G(ctx).WithError(err).Warn("stream proxy sandbox->client ended with error")
		}
	case err := <-sandboxToClient:
		if err != nil && !errors.Is(err, io.EOF) {
			log.G(ctx).WithError(err).Warn("stream proxy sandbox->client ended unexpectedly")
		}
	case <-ctx.Done():
	}

	return nil
}

type anySender interface {
	Send(*anypb.Any) error
}

type anyReceiver interface {
	Recv() (*anypb.Any, error)
}

// proxyMessages reads messages from src and sends them to dst until an
// error occurs. The error (or nil) is sent on the returned channel.
func proxyMessages(src anyReceiver, dst anySender) chan error {
	ch := make(chan error, 1)
	go func() {
		var err error
		defer func() { ch <- err }()
		for {
			var msg *anypb.Any
			msg, err = src.Recv()
			if err != nil {
				return
			}
			if err = dst.Send(msg); err != nil {
				return
			}
		}
	}()
	return ch
}
