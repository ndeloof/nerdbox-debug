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
	"io"

	streamapi "github.com/containerd/containerd/api/services/streaming/v1"
	"github.com/containerd/containerd/v2/core/streaming"
	ptypes "github.com/containerd/containerd/v2/pkg/protobuf/types"
	cplugins "github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
	"github.com/containerd/ttrpc"
	"github.com/containerd/typeurl/v2"
)

func init() {
	registry.Register(&plugin.Registration{
		Type: cplugins.TTRPCPlugin,
		ID:   "streaming",
		Requires: []plugin.Type{
			cplugins.StreamingPlugin,
		},
		InitFn: func(ic *plugin.InitContext) (any, error) {
			sp, err := ic.GetByID(cplugins.StreamingPlugin, "manager")
			if err != nil {
				return nil, err
			}
			return &streamService{
				manager: sp.(streaming.StreamManager),
			}, nil
		},
	})
}

type streamService struct {
	manager streaming.StreamManager
}

func (s *streamService) RegisterTTRPC(server *ttrpc.Server) error {
	streamapi.RegisterTTRPCStreamingService(server, s)
	return nil
}

func (s *streamService) Stream(ctx context.Context, srv streamapi.TTRPCStreaming_StreamServer) error {
	a, err := srv.Recv()
	if err != nil {
		return err
	}
	var i streamapi.StreamInit
	if err := typeurl.UnmarshalTo(a, &i); err != nil {
		return err
	}

	cc := make(chan struct{})
	ss := &serviceStream{
		s:  srv,
		cc: cc,
	}

	log.G(ctx).WithField("stream", i.ID).Debug("registering stream")
	if err := s.manager.Register(ctx, i.ID, ss); err != nil {
		return err
	}

	// Send ack after registering
	e, _ := typeurl.MarshalAnyToProto(&ptypes.Empty{})
	if err := srv.Send(e); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
	case <-cc:
	}

	return nil
}

type serviceStream struct {
	s  streamapi.TTRPCStreaming_StreamServer
	cc chan struct{}
}

func (ss *serviceStream) Send(a typeurl.Any) (err error) {
	err = ss.s.Send(typeurl.MarshalProto(a))
	if !errors.Is(err, io.EOF) {
		err = errgrpc.ToNative(err)
	}
	return
}

func (ss *serviceStream) Recv() (a typeurl.Any, err error) {
	a, err = ss.s.Recv()
	if !errors.Is(err, io.EOF) {
		err = errgrpc.ToNative(err)
	}
	return
}

func (ss *serviceStream) Close() error {
	select {
	case <-ss.cc:
	default:
		close(ss.cc)
	}
	return nil
}
