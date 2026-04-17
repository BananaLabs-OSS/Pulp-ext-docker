// Package dockerext provides the spawn.docker capability for Pulp
// plugins, backed by Potassium's Docker provider. This is the v0.7
// Spawn primitive — it gives plugins the ability to manage Docker
// containers: create, destroy, restart, exec, read logs, poll events,
// and read/write files inside containers.
//
// Bananagine (the orchestrator) is the primary consumer of this
// capability. Evolution uses the file-ops subset for whitelist and
// ops sync. Any plugin that declares "spawn.docker" in its manifest
// gets the full set.
//
// The Docker socket must be accessible to the Pulp host process
// (typically via DOCKER_HOST env var or the default Unix socket).
//
// Deployment:
//
//	import _ "github.com/BananaLabs-OSS/Pulp-ext-docker"
//
// Host imports exposed (all msgpack request/response):
//
//	docker_list(req, resp)
//	docker_create(req, resp)
//	docker_destroy(req)
//	docker_restart(req)
//	docker_exec(req, resp)
//	docker_logs(req, resp)
//	docker_stats(req, resp)
//	docker_files_read(req, resp)
//	docker_files_write(req)
//	docker_events_poll(req, resp)
package dockerext

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/bananalabs-oss/potassium/orchestrator/providers/docker"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	ext.Register(ext.Capability{
		Name:     "spawn.docker",
		Register: bindActive,
		Stub:     bindStub,
	})
}

var (
	providerMu  sync.Mutex
	provider    *docker.DockerProvider
	providerErr error
	eventBuf    *eventBuffer
)

func ensureProvider() (*docker.DockerProvider, error) {
	providerMu.Lock()
	defer providerMu.Unlock()
	if provider != nil {
		return provider, nil
	}
	if providerErr != nil {
		return nil, providerErr
	}
	p, err := docker.New()
	if err != nil {
		providerErr = fmt.Errorf("docker: %w", err)
		return nil, providerErr
	}
	provider = p

	// Start event consumer that fills the ring buffer for polling.
	eventBuf = newEventBuffer(1000, 5*time.Minute)
	go func() {
		ch, errCh := provider.Events(context.Background())
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return
				}
				eventBuf.append(ev.ContainerID, ev.Name, ev.Action)
			case <-errCh:
				return
			}
		}
	}()

	return provider, nil
}

// ---- request / response types -------------------------------------------

type listRequest struct {
	Filter map[string]string `msgpack:"filter,omitempty"`
}

type createRequest struct {
	Template  string            `msgpack:"template"`
	ServerID  string            `msgpack:"server_id,omitempty"`
	Env       map[string]string `msgpack:"env,omitempty"`
	CPULimit  float64           `msgpack:"cpu_limit,omitempty"`
	MemLimitMB int64            `msgpack:"mem_limit_mb,omitempty"`
}

type idRequest struct {
	ID string `msgpack:"id"`
}

type execRequest struct {
	ID  string   `msgpack:"id"`
	Cmd []string `msgpack:"cmd"`
}

type execResponse struct {
	Output string `msgpack:"output"`
}

type logsRequest struct {
	ID   string `msgpack:"id"`
	Tail int    `msgpack:"tail"`
}

type logsResponse struct {
	Logs string `msgpack:"logs"`
}

type filesReadRequest struct {
	ID   string `msgpack:"id"`
	Path string `msgpack:"path"`
}

type filesWriteRequest struct {
	ID   string `msgpack:"id"`
	Path string `msgpack:"path"`
	Data []byte `msgpack:"data"`
}

type eventsPollRequest struct {
	SinceNanos int64 `msgpack:"since_nanos"`
	Limit      int   `msgpack:"limit"`
}

type eventEntry struct {
	Timestamp   int64  `msgpack:"timestamp"`
	ContainerID string `msgpack:"container_id"`
	Name        string `msgpack:"name"`
	Action      string `msgpack:"action"`
}

// ---- binding ------------------------------------------------------------

func bindActive(b wazero.HostModuleBuilder, _ ext.Plugin) error {
	b.NewFunctionBuilder().WithFunc(dockerList).Export("docker_list")
	b.NewFunctionBuilder().WithFunc(dockerDestroy).Export("docker_destroy")
	b.NewFunctionBuilder().WithFunc(dockerRestart).Export("docker_restart")
	b.NewFunctionBuilder().WithFunc(dockerExec).Export("docker_exec")
	b.NewFunctionBuilder().WithFunc(dockerLogs).Export("docker_logs")
	b.NewFunctionBuilder().WithFunc(dockerStats).Export("docker_stats")
	b.NewFunctionBuilder().WithFunc(dockerFilesRead).Export("docker_files_read")
	b.NewFunctionBuilder().WithFunc(dockerFilesWrite).Export("docker_files_write")
	b.NewFunctionBuilder().WithFunc(dockerEventsPoll).Export("docker_events_poll")
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Plugin) error {
	nop4 := func(_ context.Context, _ api.Module, _, _, _, _ uint32) uint32 { return 99 }
	nop2 := func(_ context.Context, _ api.Module, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_list")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_destroy")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_restart")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_exec")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_logs")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_stats")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_files_read")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_files_write")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_events_poll")
	return nil
}

// ---- handlers -----------------------------------------------------------

func dockerList(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	var req listRequest
	if reqLen > 0 {
		data, ok := m.Memory().Read(reqPtr, reqLen)
		if !ok {
			return 2
		}
		if err := msgpack.Unmarshal(data, &req); err != nil {
			return 3
		}
	}
	servers, err := p.List(ctx, req.Filter)
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, servers, respPtrOut, respLenOut)
}

func dockerDestroy(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req idRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := p.Deallocate(ctx, req.ID); err != nil {
		return 4
	}
	return 0
}

func dockerRestart(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req idRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := p.Restart(ctx, req.ID); err != nil {
		return 4
	}
	return 0
}

func dockerExec(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req execRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	output, err := p.Exec(ctx, req.ID, req.Cmd)
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, execResponse{Output: output}, respPtrOut, respLenOut)
}

func dockerLogs(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req logsRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if req.Tail <= 0 {
		req.Tail = 200
	}
	logs, err := p.Logs(ctx, req.ID, req.Tail)
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, logsResponse{Logs: logs}, respPtrOut, respLenOut)
}

func dockerStats(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req idRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	stats, err := p.Stats(ctx, req.ID)
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, stats, respPtrOut, respLenOut)
}

func dockerFilesRead(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req filesReadRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	content, err := p.CopyFrom(ctx, req.ID, req.Path)
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, content, respPtrOut, respLenOut)
}

func dockerFilesWrite(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return 10
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req filesWriteRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := p.CopyTo(ctx, req.ID, req.Path, req.Data); err != nil {
		return 4
	}
	return 0
}

func dockerEventsPoll(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if _, err := ensureProvider(); err != nil {
		return 10
	}
	var req eventsPollRequest
	if reqLen > 0 {
		data, ok := m.Memory().Read(reqPtr, reqLen)
		if !ok {
			return 2
		}
		if err := msgpack.Unmarshal(data, &req); err != nil {
			return 3
		}
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	events := eventBuf.since(req.SinceNanos, req.Limit)
	return writeMsgpackResponse(ctx, m, events, respPtrOut, respLenOut)
}

// writeMsgpackResponse encodes v → plugin memory via pulp_alloc.
func writeMsgpackResponse(ctx context.Context, m api.Module, v any, respPtrOut, respLenOut uint32) uint32 {
	encoded, err := msgpack.Marshal(v)
	if err != nil {
		return 5
	}
	allocFn := m.ExportedFunction("pulp_alloc")
	if allocFn == nil {
		return 7
	}
	var ptr uint32
	if len(encoded) > 0 {
		res, err := allocFn.Call(ctx, uint64(len(encoded)))
		if err != nil || len(res) == 0 {
			return 7
		}
		ptr = uint32(res[0])
		if ptr == 0 {
			return 7
		}
		if !m.Memory().Write(ptr, encoded) {
			return 8
		}
	}
	if !m.Memory().WriteUint32Le(respPtrOut, ptr) {
		return 8
	}
	if !m.Memory().WriteUint32Le(respLenOut, uint32(len(encoded))) {
		return 8
	}
	return 0
}

// ---- inline event ring buffer -------------------------------------------

type eventBuffer struct {
	mu       sync.Mutex
	events   []eventEntry
	maxSize  int
	maxAgeNs int64
}

func newEventBuffer(maxSize int, maxAge time.Duration) *eventBuffer {
	return &eventBuffer{
		events:   make([]eventEntry, 0, maxSize),
		maxSize:  maxSize,
		maxAgeNs: maxAge.Nanoseconds(),
	}
}

func (b *eventBuffer) append(containerID, name, action string) {
	now := time.Now().UnixNano()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, eventEntry{
		Timestamp:   now,
		ContainerID: containerID,
		Name:        name,
		Action:      action,
	})
	b.prune(now)
}

func (b *eventBuffer) since(sinceNanos int64, limit int) []eventEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(time.Now().UnixNano())
	out := make([]eventEntry, 0, 16)
	for _, e := range b.events {
		if e.Timestamp > sinceNanos {
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out
}

func (b *eventBuffer) prune(now int64) {
	cutoff := now - b.maxAgeNs
	i := 0
	for i < len(b.events) && b.events[i].Timestamp <= cutoff {
		i++
	}
	if i > 0 {
		b.events = b.events[i:]
	}
	if len(b.events) > b.maxSize {
		b.events = b.events[len(b.events)-b.maxSize:]
	}
}

// ---- env logging --------------------------------------------------------

func init() {
	if os.Getenv("DOCKER_HOST") != "" {
		fmt.Printf("[pulp-ext-docker] DOCKER_HOST=%s\n", os.Getenv("DOCKER_HOST"))
	}
}
