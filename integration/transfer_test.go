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

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	streamapi "github.com/containerd/containerd/api/services/streaming/v1"
	transferapi "github.com/containerd/containerd/api/services/transfer/v1"
	"github.com/containerd/containerd/v2/core/streaming"
	"github.com/containerd/typeurl/v2"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/containerd/nerdbox/internal/transfer"
	"github.com/containerd/nerdbox/internal/vm"
)

func TestTransferEcho(t *testing.T) {
	runWithVM(t, func(t *testing.T, i vm.Instance) {
		ctx := t.Context()
		client := i.Client()

		// Generate random test data
		testData := make([]byte, 64*1024) // 64KB
		if _, err := rand.Read(testData); err != nil {
			t.Fatal("failed to generate test data:", err)
		}

		// Create a stream creator backed by TTRPC streams to the VM
		sc := &ttrpcStreamCreator{
			client: streamapi.NewTTRPCStreamingClient(client),
		}

		// Source: ReadStream sending testData
		src := transfer.NewReadStream(bytes.NewReader(testData), "application/octet-stream")

		// Destination: WriteStream receiving into a buffer.
		// Use a signaling writer so we can wait for all stream data to
		// arrive before checking — the ReceiveStream goroutine may still
		// be writing after the Transfer RPC returns.
		var received bytes.Buffer
		dstWriter := &doneWriter{Writer: &received, done: make(chan struct{})}
		dst := transfer.NewWriteStream(dstWriter, "application/octet-stream")

		// Marshal both (this creates streams and starts data pump goroutines)
		srcAny, err := marshalTransferAny(ctx, src, sc)
		if err != nil {
			t.Fatal("failed to marshal source:", err)
		}
		dstAny, err := marshalTransferAny(ctx, dst, sc)
		if err != nil {
			t.Fatal("failed to marshal destination:", err)
		}

		// Call Transfer via TTRPC
		tc := transferapi.NewTTRPCTransferClient(client)
		if _, err := tc.Transfer(ctx, &transferapi.TransferRequest{
			Source:      srcAny,
			Destination: dstAny,
		}); err != nil {
			t.Fatal("transfer failed:", err)
		}

		// Wait for the receive goroutine to finish draining the stream.
		<-dstWriter.done

		// Verify
		if !bytes.Equal(received.Bytes(), testData) {
			t.Fatalf("data mismatch: sent %d bytes, received %d bytes", len(testData), received.Len())
		}
		t.Logf("echo transfer: %d bytes transferred successfully", len(testData))
	})
}

// streamMarshaler is the interface implemented by types that need to create
// streams during marshaling (e.g. ReadStream, WriteStream).
type streamMarshaler interface {
	MarshalAny(context.Context, streaming.StreamCreator) (typeurl.Any, error)
}

// marshalTransferAny marshals a transfer type, using the stream creator if
// the type implements streamMarshaler, otherwise using plain typeurl marshal.
func marshalTransferAny(ctx context.Context, v any, sc streaming.StreamCreator) (*anypb.Any, error) {
	var a typeurl.Any
	var err error
	if sm, ok := v.(streamMarshaler); ok {
		a, err = sm.MarshalAny(ctx, sc)
	} else {
		a, err = typeurl.MarshalAny(v)
	}
	if err != nil {
		return nil, err
	}
	return &anypb.Any{
		TypeUrl: a.GetTypeUrl(),
		Value:   a.GetValue(),
	}, nil
}

// ttrpcStreamCreator implements streaming.StreamCreator by opening TTRPC
// streaming RPCs to the VM.
type ttrpcStreamCreator struct {
	client streamapi.TTRPCStreamingClient
}

func (sc *ttrpcStreamCreator) Create(ctx context.Context, id string) (streaming.Stream, error) {
	stream, err := sc.client.Stream(ctx)
	if err != nil {
		return nil, err
	}

	// Send StreamInit
	init, err := typeurl.MarshalAnyToProto(&streamapi.StreamInit{ID: id})
	if err != nil {
		stream.CloseSend()
		return nil, err
	}
	if err := stream.Send(init); err != nil {
		stream.CloseSend()
		return nil, err
	}

	// Receive ack
	if _, err := stream.Recv(); err != nil {
		stream.CloseSend()
		return nil, err
	}

	return &clientStream{s: stream}, nil
}

// clientStream wraps a TTRPCStreaming_StreamClient to implement streaming.Stream.
type clientStream struct {
	s streamapi.TTRPCStreaming_StreamClient
}

func (cs *clientStream) Send(a typeurl.Any) error {
	return cs.s.Send(typeurl.MarshalProto(a))
}

func (cs *clientStream) Recv() (typeurl.Any, error) {
	return cs.s.Recv()
}

func (cs *clientStream) Close() error {
	return cs.s.CloseSend()
}

// doneWriter wraps an io.Writer and signals on the done channel when
// Close is called. This lets callers wait for the ReceiveStream
// goroutine (which calls Close after draining all data) to finish.
type doneWriter struct {
	io.Writer
	done chan struct{}
}

func (w *doneWriter) Close() error {
	close(w.done)
	return nil
}
