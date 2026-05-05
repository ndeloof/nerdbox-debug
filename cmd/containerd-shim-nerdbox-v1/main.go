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

package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/log"

	"github.com/containerd/nerdbox/internal/shim/manager"

	_ "github.com/containerd/nerdbox/plugins/shim/sandbox"
	_ "github.com/containerd/nerdbox/plugins/shim/streaming"
	_ "github.com/containerd/nerdbox/plugins/shim/task"
	_ "github.com/containerd/nerdbox/plugins/shim/transfer"
	_ "github.com/containerd/nerdbox/plugins/vm/libkrun"
)

// startHeartbeat spawns a goroutine that logs a "still alive" heartbeat every
// 2 seconds for the lifetime of the process. CI runs of the shim sometimes
// stop emitting any output mid-RPC (e.g. after "deleting task") with no
// shutdown / signal / panic logs. The heartbeat distinguishes:
//   - shim is still running but stuck (heartbeats keep coming, no other logs)
//   - shim exited cleanly via a known path (heartbeats stop AND a shutdown,
//     signal-received, panic, or fatal-exit log line is emitted)
//   - shim was killed externally by the OS (heartbeats stop with NO other
//     log line — only an external killer can produce this signature)
//
// See docker/sandboxes#2529 and the dockerd_diagnostic branch for the
// investigation that motivated this instrumentation.
func startHeartbeat(ctx context.Context) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.G(ctx).WithField("stack", string(debug.Stack())).Errorf("shim-panic: heartbeat goroutine panicked: %v", r)
			}
		}()
		pid := os.Getpid()
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				log.G(ctx).WithField("pid", pid).Info("shim-heartbeat: context canceled, stopping")
				return
			case <-t.C:
				log.G(ctx).WithFields(log.Fields{
					"pid":        pid,
					"goroutines": runtime.NumGoroutine(),
				}).Info("shim-heartbeat: alive")
			}
		}
	}()
}

// startSignalLogger registers a handler for the catchable termination signals
// (os.Interrupt and syscall.SIGTERM — os.Kill / SIGKILL cannot be caught on
// any platform, but its absence here is itself a signal: if the shim
// disappears with no "shim-signal-received" log, it was killed by SIGKILL
// or an equivalent uncatchable OS-level kill).
//
// We do NOT swallow the signal: after logging, we re-emit the default
// behaviour by calling os.Exit with the conventional 128+signo status, so
// that downstream cleanup (defers in main, etc.) is at least observable
// from the log trail rather than blocked by a custom handler.
func startSignalLogger(ctx context.Context) {
	c := make(chan os.Signal, 4)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.G(ctx).WithField("stack", string(debug.Stack())).Errorf("shim-panic: signal logger goroutine panicked: %v", r)
			}
		}()
		for sig := range c {
			log.G(ctx).WithFields(log.Fields{
				"signal": sig.String(),
				"pid":    os.Getpid(),
			}).Warn("shim-signal-received")
			// Re-raise via clean exit. 128+signal-number is the conventional
			// shell exit status for signal-terminated processes.
			code := 128
			if s, ok := sig.(syscall.Signal); ok {
				code = 128 + int(s)
			}
			log.G(ctx).WithField("exitCode", code).Warn("shim-fatal-exit: exiting due to signal")
			os.Exit(code)
		}
	}()
}

func main() {
	// Diagnostic instrumentation for docker/sandboxes#2529: heartbeat,
	// signal logging, and panic logging in main(). These run for the full
	// process lifetime and emit before any RunShim subcommand dispatch so
	// that even short-lived "delete" / "start" subcommand invocations get
	// at least one heartbeat tick when they hang.
	ctx := context.Background()
	startHeartbeat(ctx)
	startSignalLogger(ctx)

	defer func() {
		if r := recover(); r != nil {
			log.G(ctx).WithField("stack", string(debug.Stack())).Errorf("shim-panic: main panicked: %v", r)
			// Re-panic so the runtime still dumps the original stack and
			// the process exits with the standard panic exit code.
			panic(r)
		}
	}()

	shim.RunShim(ctx, manager.NewShimManager("io.containerd.nerdbox.v1"))
}
