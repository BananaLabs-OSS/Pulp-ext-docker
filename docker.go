// Package dockerext provides the spawn.docker capability for Pulp
// cells, backed by Potassium's Docker provider. This is the v0.7
// Spawn primitive — it gives cells the ability to manage Docker
// containers: create, destroy, restart, exec, read logs, poll events,
// and read/write files inside containers.
//
// Bananagine (the orchestrator) is the primary consumer of this
// capability. Evolution uses the file-ops subset for whitelist and
// ops sync. Any cell that declares "spawn.docker" in its manifest
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
//	docker_get(req, resp)
//	docker_create(req, resp)
//	docker_destroy(req)
//	docker_restart(req)
//	docker_exec(req, resp)
//	docker_logs(req, resp)
//	docker_stats(req, resp)
//	docker_files_read(req, resp)
//	docker_files_write(req)
//	docker_files_delete(req)
//	docker_events_poll(req, resp)
package dockerext

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/bananalabs-oss/potassium/orchestrator"
	"github.com/bananalabs-oss/potassium/orchestrator/providers/docker"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// Host return codes. Kept stable — Fiber's pulp/docker wrapper maps
// these to typed errors.
//
//	0   success
//	1   invalid request (missing required field)
//	2   memory read failed
//	3   msgpack decode failed
//	4   docker provider/API error
//	5   msgpack encode failed
//	6   resource not found
//	7   pulp_alloc missing or returned 0
//	8   memory write failed
//	10  docker provider not available (socket unreachable)
//	11  build already in progress
//	99  capability not declared in cell manifest (stubbed)
const (
	codeOK                = 0
	codeInvalidRequest    = 1
	codeMemoryRead        = 2
	codeMsgpackDecode     = 3
	codeDockerError       = 4
	codeMsgpackEncode     = 5
	codeNotFound          = 6
	codeAllocFailed       = 7
	codeMemoryWrite       = 8
	codeProviderUnavail   = 10
	codeBuildInProgress   = 11
	codeCapabilityStubbed = 99
)

// isNotFound reports whether a Docker provider error represents a
// missing container/image. Docker's SDK wraps the API's 404 response
// as an error whose message contains "No such container" or "No such
// image" — there's no exported errdef for this in the client surface.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "No such container") ||
		strings.Contains(msg, "No such image") ||
		strings.Contains(msg, "no such container") ||
		errors.Is(err, os.ErrNotExist)
}

// translateDockerErr returns a host code for a provider error,
// distinguishing not-found from generic failure.
func translateDockerErr(err error) uint32 {
	if isNotFound(err) {
		return codeNotFound
	}
	return codeDockerError
}

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

	buildMu      sync.Mutex
	buildStateMu sync.RWMutex
	buildState   buildStatusResponse
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
	// Docker's SDK hands back channels immediately, so we can't use
	// the channel call itself to confirm a good subscription —
	// backoff is only reset after we actually receive an event.
	eventBuf = newEventBuffer(1000, 5*time.Minute)
	go func() {
		ctx := context.Background()
		backoff := time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			ch, errCh := provider.Events(ctx)
			gotEvent := false
		consume:
			for {
				select {
				case ev, ok := <-ch:
					if !ok {
						log.Printf("[pulp-ext-docker] events channel closed, reconnecting")
						break consume
					}
					eventBuf.append(ev.ContainerID, ev.Name, ev.Action)
					if !gotEvent {
						backoff = time.Second
						gotEvent = true
					}
				case err, ok := <-errCh:
					if ok && err != nil {
						log.Printf("[pulp-ext-docker] events error: %v", err)
					} else {
						log.Printf("[pulp-ext-docker] events errCh closed, reconnecting")
					}
					break consume
				case <-ctx.Done():
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
	}()

	return provider, nil
}

// ---- request / response types -------------------------------------------

type listRequest struct {
	Filter map[string]string `msgpack:"filter,omitempty"`
}

type portBinding struct {
	Host      int    `msgpack:"host"`
	Container int    `msgpack:"container"`
	Protocol  string `msgpack:"protocol"`
	Name      string `msgpack:"name,omitempty"`
	Range     string `msgpack:"range,omitempty"`
}

type createRequest struct {
	Image          string            `msgpack:"image"`
	Name           string            `msgpack:"name,omitempty"`
	Environment    map[string]string `msgpack:"environment,omitempty"`
	Volumes        map[string]string `msgpack:"volumes,omitempty"`
	Ports          []portBinding     `msgpack:"ports,omitempty"`
	Network        string            `msgpack:"network,omitempty"`
	IP             string            `msgpack:"ip,omitempty"`
	MemoryLimit    int64             `msgpack:"memory_limit,omitempty"`
	CPULimit       float64           `msgpack:"cpu_limit,omitempty"`
	DiskIOReadBps  int64             `msgpack:"disk_io_read_bps,omitempty"`
	DiskIOWriteBps int64             `msgpack:"disk_io_write_bps,omitempty"`
	DiskSizeLimit  int64             `msgpack:"disk_size_limit,omitempty"`
	PidsLimit      int64             `msgpack:"pids_limit,omitempty"`
	MemorySwap     int64             `msgpack:"memory_swap,omitempty"`
}

type createResponse struct {
	ID          string         `msgpack:"id"`
	Name        string         `msgpack:"name"`
	Status      string         `msgpack:"status"`
	IP          string         `msgpack:"ip"`
	Ports       map[string]int `msgpack:"ports"`
	CPULimit    float64        `msgpack:"cpu_limit,omitempty"`
	MemoryLimit int64          `msgpack:"memory_limit,omitempty"`
}

type idRequest struct {
	ID string `msgpack:"id"`
}

type nameRequest struct {
	Name string `msgpack:"name"`
}

type filesDeleteRequest struct {
	Container string `msgpack:"container"`
	Path      string `msgpack:"path"`
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

type statsResponse struct {
	ContainerID    string  `msgpack:"container_id"`
	Name           string  `msgpack:"name"`
	CPUPercent     float64 `msgpack:"cpu_percent"`
	MemoryUsed     int64   `msgpack:"memory_used"`
	MemoryLimit    int64   `msgpack:"memory_limit"`
	NetRxBytes     int64   `msgpack:"net_rx_bytes"`
	NetTxBytes     int64   `msgpack:"net_tx_bytes"`
	DiskReadBytes  int64   `msgpack:"disk_read_bytes"`
	DiskWriteBytes int64   `msgpack:"disk_write_bytes"`
	Timestamp      int64   `msgpack:"timestamp"`
}

type buildRequest struct {
	BuildArgs map[string]string `msgpack:"build_args,omitempty"`
	ImageTag  string            `msgpack:"image_tag"`
	BuildDir  string            `msgpack:"build_dir"`
}

type buildStatusResponse struct {
	Building      bool   `msgpack:"building"`
	LastBuildTime int64  `msgpack:"last_build_time"`
	LastError     string `msgpack:"last_error,omitempty"`
}

func serverToResponse(s *orchestrator.Server) createResponse {
	return createResponse{
		ID:          s.ID,
		Name:        s.Name,
		Status:      string(s.Status),
		IP:          s.IP,
		Ports:       s.Ports,
		CPULimit:    s.CPULimit,
		MemoryLimit: s.MemoryLimit,
	}
}

func containerStatsToResponse(s *docker.ContainerStats) statsResponse {
	return statsResponse{
		ContainerID:    s.ContainerID,
		Name:           s.Name,
		CPUPercent:     s.CPUPercent,
		MemoryUsed:     s.MemoryUsed,
		MemoryLimit:    s.MemoryLimit,
		NetRxBytes:     s.NetRxBytes,
		NetTxBytes:     s.NetTxBytes,
		DiskReadBytes:  s.DiskReadBytes,
		DiskWriteBytes: s.DiskWriteBytes,
		Timestamp:      s.Timestamp,
	}
}

// ---- security: bind-mount allowlist -------------------------------------
//
// docker_create forwards createRequest.Volumes verbatim into the provider,
// which builds Docker binds as host+":"+container with NO validation. A cell
// could request {"/var/run/docker.sock":...} or {"/":"/host"} and then exec
// into the resulting container — that is a complete host takeover (socket-in-
// container trivially spawns a privileged container mounting /).
//
// This guard mirrors the egress-guard pattern in Pulp-ext-http: a deny-list of
// sensitive host paths/prefixes that can NEVER be a bind source, plus an
// allow-root model — every bind source must resolve (after symlink eval) to a
// path under one of the designated safe mount roots. Default is deny-all
// unless DOCKER_BIND_ROOTS is configured, so a misconfigured deployment fails
// closed rather than exposing the host.

// errBlockedMount is returned when a requested bind-mount source is rejected.
var errBlockedMount = errors.New("docker bind-mount guard: source path is not under an allowed mount root")

// deniedMountPaths are host paths/prefixes that may NEVER be bind-mounted,
// regardless of the configured allow-roots. They are checked against the
// symlink-resolved, cleaned absolute source. Entries are matched as the path
// itself OR any path nested beneath them (so /etc also denies /etc/passwd).
//
// NOTE: the host root "/" is deliberately NOT a prefix entry — a prefix match
// on "/" would reject EVERY absolute bind source on Linux (the trailing-
// separator branch of pathContains makes "/" a prefix of all absolute paths),
// which would block every legitimate world mount (e.g. /var/worlds/...,
// /var/sessions/worlds/...). The whole-host "/" mount is instead caught by the
// exact-match deny in validateMountSource. Segment-aware prefix matching keeps
// these segment-boundary-distinct: /var/run is denied but /var/worlds and
// /var/sessions (the real provisioning roots) are NOT, even though all share
// the /var parent.
var deniedMountPaths = []string{
	"/var/run/docker.sock",
	"/run/docker.sock",
	"/var/run",
	"/run",
	"/etc",
	"/proc",
	"/sys",
	"/dev",
	"/boot",
	"/root",
	"/var/lib/docker",
}

// deniedExactMountPaths are host paths that may never be bind-mounted as a
// WHOLE-PATH source but must not be treated as prefixes. The host root "/"
// lives here: mounting "/" itself is a full-host takeover, but using "/" as a
// prefix would (wrongly) reject every absolute path.
var deniedExactMountPaths = []string{
	"/",
}

// pathContains reports whether target == base or target is nested under base.
// Both are expected to be cleaned absolute paths. The separator check prevents
// the sibling-prefix bypass (/etcfoo is NOT under /etc).
//
// The path separator is taken from base so the check is correct on whichever
// OS produced the paths: the unix deny-list ("/etc", …) compares against unix
// sources, while a Windows-rooted allow-root compares against Windows sources.
func pathContains(base, target string) bool {
	if base == target {
		return true
	}
	sep := "/"
	if strings.ContainsRune(base, '\\') || (len(base) >= 2 && base[1] == ':') {
		sep = string(filepath.Separator)
	}
	// A pure-root base ("/" or "C:\") already ends in the separator; appending
	// another would over-match. Normalize to a single trailing separator.
	if strings.HasSuffix(base, sep) {
		return strings.HasPrefix(target, base)
	}
	return strings.HasPrefix(target, base+sep)
}

// allowedMountRoots returns the configured safe mount roots from
// DOCKER_BIND_ROOTS (OS-path-list separated, ':' on unix). Empty/unset means
// no roots are allowed → all bind-mounts are rejected (fail closed).
func allowedMountRoots() []string {
	raw := os.Getenv("DOCKER_BIND_ROOTS")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var roots []string
	for _, entry := range filepath.SplitList(raw) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		abs, err := filepath.Abs(entry)
		if err != nil {
			continue
		}
		// Resolve symlinks where possible so the comparison base matches the
		// resolved source; fall back to the cleaned abs path if the root does
		// not yet exist.
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		}
		roots = append(roots, filepath.Clean(abs))
	}
	return roots
}

// validateMountSource reports whether a single bind source host path is safe to
// mount. It rejects the deny-list and anything not under an allowed root.
func validateMountSource(src string, roots []string) error {
	if strings.TrimSpace(src) == "" {
		return fmt.Errorf("%w: empty source", errBlockedMount)
	}
	abs, err := filepath.Abs(src)
	if err != nil {
		return fmt.Errorf("%w: %v", errBlockedMount, err)
	}
	abs = filepath.Clean(abs)
	// Resolve symlinks so a symlink inside an allowed root that points at /etc
	// (or the docker socket) cannot slip past the deny-list / root check.
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = filepath.Clean(resolved)
	}
	for _, denied := range deniedExactMountPaths {
		if abs == denied {
			return fmt.Errorf("%w: %s is a protected host path", errBlockedMount, abs)
		}
	}
	for _, denied := range deniedMountPaths {
		if pathContains(denied, abs) {
			return fmt.Errorf("%w: %s is a protected host path", errBlockedMount, abs)
		}
	}
	for _, root := range roots {
		if pathContains(root, abs) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", errBlockedMount, abs)
}

// validateVolumes checks every bind source in a create request. Returns the
// first violation, or nil if all sources are permitted (or there are none).
func validateVolumes(volumes map[string]string) error {
	if len(volumes) == 0 {
		return nil
	}
	roots := allowedMountRoots()
	for src := range volumes {
		if err := validateMountSource(src, roots); err != nil {
			return err
		}
	}
	return nil
}

// ---- security: per-cell container ownership scoping ---------------------
//
// The spawn.docker capability is otherwise all-or-nothing: any cell holding it
// could destroy/exec/read-files/restart ANY container on the host, including
// sibling products and host infra. We scope cell-targetable operations to
// containers THIS cell created, identified by a deterministic name prefix
// stamped at create time (mirroring how ext-sqlite keys state by cellID).
//
// Bananagine is the legitimate whole-host orchestrator; it sets
// DOCKER_SCOPE_DISABLE=1 to retain unscoped control. With scoping enabled
// (the default), create() forces the container name into the cell's namespace
// and the mutating handlers refuse targets that don't carry the caller's prefix.

// scopingDisabled reports whether per-cell container scoping is turned off
// (whole-host mode for a trusted sole orchestrator like Bananagine).
func scopingDisabled() bool {
	v := strings.TrimSpace(os.Getenv("DOCKER_SCOPE_DISABLE"))
	return v == "1" || strings.EqualFold(v, "true")
}

// sanitizeCellID maps a cell name to a Docker-name-safe token. Docker names
// must match [a-zA-Z0-9][a-zA-Z0-9_.-]+; cell names are operator-authored but
// we normalize defensively so the prefix is always a legal, separator-free
// token (also closes any path/name-injection via an exotic cell name).
func sanitizeCellID(cellID string) string {
	var b strings.Builder
	for _, r := range cellID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '.' || r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" {
		s = "cell"
	}
	return s
}

// cellPrefix is the container-name namespace for a cell: "pulp-<cellID>-".
func cellPrefix(cellID string) string {
	return "pulp-" + sanitizeCellID(cellID) + "-"
}

// nameOwnedByCell reports whether a container name belongs to cellID. Docker
// prefixes inspected names with a leading "/", which we trim before testing.
func nameOwnedByCell(name, cellID string) bool {
	name = strings.TrimPrefix(name, "/")
	return strings.HasPrefix(name, cellPrefix(cellID))
}

// authorizeTarget verifies that the container identified by target (ID or name)
// is owned by cellID. It returns codeOK when allowed, a non-OK host code
// otherwise. Scoping is skipped entirely when DOCKER_SCOPE_DISABLE is set.
func authorizeTarget(ctx context.Context, p *docker.DockerProvider, cellID, target string) uint32 {
	if scopingDisabled() {
		return codeOK
	}
	// A bare prefix-named target the cell supplied is trusted only after we
	// confirm the daemon agrees the container exists under that name. Resolve
	// via the provider (accepts ID or name) and check the canonical name.
	server, err := p.Get(ctx, target)
	if err != nil {
		return translateDockerErr(err)
	}
	if !nameOwnedByCell(server.Name, cellID) {
		// Don't reveal existence of containers the cell doesn't own — report
		// not-found rather than a distinct "forbidden" code.
		return codeNotFound
	}
	return codeOK
}

// ---- binding ------------------------------------------------------------

// Container operations are scoped per cell via a name-prefix ownership check
// (see authorizeTarget) and bind-mounts are restricted to an allowed root set
// (see validateVolumes). The legacy whole-host behaviour — any spawn.docker
// cell managing ALL containers — is available only behind DOCKER_SCOPE_DISABLE
// for a trusted sole orchestrator (Bananagine).
func bindActive(b wazero.HostModuleBuilder, cell ext.Cell) error {
	cellID := cell.Name()
	wrap4 := func(h func(context.Context, api.Module, string, uint32, uint32, uint32, uint32) uint32) func(context.Context, api.Module, uint32, uint32, uint32, uint32) uint32 {
		return func(ctx context.Context, m api.Module, a, bb, c, d uint32) uint32 {
			return h(ctx, m, cellID, a, bb, c, d)
		}
	}
	wrap2 := func(h func(context.Context, api.Module, string, uint32, uint32) uint32) func(context.Context, api.Module, uint32, uint32) uint32 {
		return func(ctx context.Context, m api.Module, a, bb uint32) uint32 {
			return h(ctx, m, cellID, a, bb)
		}
	}
	b.NewFunctionBuilder().WithFunc(dockerList).Export("docker_list")
	b.NewFunctionBuilder().WithFunc(dockerGet).Export("docker_get")
	b.NewFunctionBuilder().WithFunc(wrap4(dockerCreate)).Export("docker_create")
	b.NewFunctionBuilder().WithFunc(wrap2(dockerDestroy)).Export("docker_destroy")
	b.NewFunctionBuilder().WithFunc(wrap2(dockerRestart)).Export("docker_restart")
	b.NewFunctionBuilder().WithFunc(wrap4(dockerExec)).Export("docker_exec")
	b.NewFunctionBuilder().WithFunc(wrap4(dockerLogs)).Export("docker_logs")
	b.NewFunctionBuilder().WithFunc(wrap4(dockerStats)).Export("docker_stats")
	b.NewFunctionBuilder().WithFunc(wrap4(dockerFilesRead)).Export("docker_files_read")
	b.NewFunctionBuilder().WithFunc(wrap2(dockerFilesWrite)).Export("docker_files_write")
	b.NewFunctionBuilder().WithFunc(wrap2(dockerFilesDelete)).Export("docker_files_delete")
	b.NewFunctionBuilder().WithFunc(dockerEventsPoll).Export("docker_events_poll")
	b.NewFunctionBuilder().WithFunc(dockerStatsAll).Export("docker_stats_all")
	b.NewFunctionBuilder().WithFunc(dockerBuild).Export("docker_build")
	b.NewFunctionBuilder().WithFunc(dockerBuildStatus).Export("docker_build_status")
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop4 := func(_ context.Context, _ api.Module, _, _, _, _ uint32) uint32 { return 99 }
	nop2 := func(_ context.Context, _ api.Module, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_list")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_get")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_create")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_destroy")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_restart")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_exec")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_logs")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_stats")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_files_read")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_files_write")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_files_delete")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_events_poll")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_stats_all")
	b.NewFunctionBuilder().WithFunc(nop2).Export("docker_build")
	b.NewFunctionBuilder().WithFunc(nop4).Export("docker_build_status")
	return nil
}

// ---- handlers -----------------------------------------------------------

func dockerList(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	var req listRequest
	if reqLen > 0 {
		data, ok := m.Memory().Read(reqPtr, reqLen)
		if !ok {
			return codeMemoryRead
		}
		if err := msgpack.Unmarshal(data, &req); err != nil {
			return codeMsgpackDecode
		}
	}
	servers, err := p.List(ctx, req.Filter)
	if err != nil {
		return translateDockerErr(err)
	}
	if len(servers) == 0 {
		m.Memory().WriteUint32Le(respPtrOut, 0)
		m.Memory().WriteUint32Le(respLenOut, 0)
		return codeOK
	}
	result := make([]createResponse, len(servers))
	for i, s := range servers {
		result[i] = serverToResponse(&s)
	}
	return writeMsgpackResponse(ctx, m, result, respPtrOut, respLenOut)
}

func dockerGet(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req nameRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.Name == "" {
		return codeInvalidRequest
	}
	server, err := p.Get(ctx, req.Name)
	if err != nil {
		return translateDockerErr(err)
	}
	return writeMsgpackResponse(ctx, m, serverToResponse(server), respPtrOut, respLenOut)
}

func dockerCreate(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req createRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.Image == "" {
		return codeInvalidRequest
	}
	// Reject host-takeover bind-mounts (docker socket, /, system dirs, and
	// anything outside the configured safe mount roots).
	if err := validateVolumes(req.Volumes); err != nil {
		return codeInvalidRequest
	}
	// Stamp the cell's ownership namespace onto the container name so later
	// destroy/exec/files/restart/logs/stats can verify the caller owns the
	// target. Skipped only in whole-host mode (DOCKER_SCOPE_DISABLE).
	if !scopingDisabled() {
		prefix := cellPrefix(cellID)
		trimmed := strings.TrimPrefix(req.Name, "/")
		if !strings.HasPrefix(trimmed, prefix) {
			req.Name = prefix + trimmed
		} else {
			req.Name = trimmed
		}
	}
	ports := make([]orchestrator.PortBinding, len(req.Ports))
	for i, pb := range req.Ports {
		ports[i] = orchestrator.PortBinding{
			Host:      pb.Host,
			Container: pb.Container,
			Protocol:  pb.Protocol,
			Name:      pb.Name,
			Range:     pb.Range,
		}
	}
	server, err := p.Allocate(ctx, orchestrator.AllocateRequest{
		Image:          req.Image,
		Name:           req.Name,
		Environment:    req.Environment,
		Volumes:        req.Volumes,
		Ports:          ports,
		Network:        req.Network,
		IP:             req.IP,
		MemoryLimit:    req.MemoryLimit,
		CPULimit:       req.CPULimit,
		DiskIOReadBps:  req.DiskIOReadBps,
		DiskIOWriteBps: req.DiskIOWriteBps,
		DiskSizeLimit:  req.DiskSizeLimit,
		PidsLimit:      req.PidsLimit,
		MemorySwap:     req.MemorySwap,
	})
	if err != nil {
		return translateDockerErr(err)
	}
	return writeMsgpackResponse(ctx, m, serverToResponse(server), respPtrOut, respLenOut)
}

func dockerDestroy(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req idRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	if err := p.Deallocate(ctx, req.ID); err != nil {
		return translateDockerErr(err)
	}
	return codeOK
}

func dockerRestart(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req idRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	if err := p.Restart(ctx, req.ID); err != nil {
		return translateDockerErr(err)
	}
	return codeOK
}

func dockerExec(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req execRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" || len(req.Cmd) == 0 {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	output, err := p.Exec(ctx, req.ID, req.Cmd)
	if err != nil {
		return translateDockerErr(err)
	}
	return writeMsgpackResponse(ctx, m, execResponse{Output: output}, respPtrOut, respLenOut)
}

func dockerLogs(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req logsRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	// Potassium's Logs passes strconv.Itoa(tail) straight to Docker,
	// which interprets "0" as zero lines (NOT "all"). Normalize: any
	// non-positive input maps to a sensible 200-line default so cells
	// don't silently receive empty strings.
	if req.Tail <= 0 {
		req.Tail = 200
	}
	logs, err := p.Logs(ctx, req.ID, req.Tail)
	if err != nil {
		return translateDockerErr(err)
	}
	return writeMsgpackResponse(ctx, m, logsResponse{Logs: logs}, respPtrOut, respLenOut)
}

func dockerStats(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req idRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	stats, err := p.Stats(ctx, req.ID)
	if err != nil {
		return translateDockerErr(err)
	}
	return writeMsgpackResponse(ctx, m, containerStatsToResponse(stats), respPtrOut, respLenOut)
}

func dockerFilesRead(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req filesReadRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" || req.Path == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	content, err := p.CopyFrom(ctx, req.ID, req.Path)
	if err != nil {
		return translateDockerErr(err)
	}
	return writeMsgpackResponse(ctx, m, content, respPtrOut, respLenOut)
}

func dockerFilesWrite(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req filesWriteRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.ID == "" || req.Path == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.ID); code != codeOK {
		return code
	}
	if err := p.CopyTo(ctx, req.ID, req.Path, req.Data); err != nil {
		return translateDockerErr(err)
	}
	return codeOK
}

func dockerFilesDelete(ctx context.Context, m api.Module, cellID string, reqPtr, reqLen uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return codeMemoryRead
	}
	var req filesDeleteRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return codeMsgpackDecode
	}
	if req.Container == "" || req.Path == "" {
		return codeInvalidRequest
	}
	if code := authorizeTarget(ctx, p, cellID, req.Container); code != codeOK {
		return code
	}
	if err := p.DeleteFile(ctx, req.Container, req.Path); err != nil {
		return translateDockerErr(err)
	}
	return codeOK
}

func dockerEventsPoll(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if _, err := ensureProvider(); err != nil {
		return codeProviderUnavail
	}
	var req eventsPollRequest
	if reqLen > 0 {
		data, ok := m.Memory().Read(reqPtr, reqLen)
		if !ok {
			return codeMemoryRead
		}
		if err := msgpack.Unmarshal(data, &req); err != nil {
			return codeMsgpackDecode
		}
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	events := eventBuf.since(req.SinceNanos, req.Limit)
	return writeMsgpackResponse(ctx, m, events, respPtrOut, respLenOut)
}

func dockerStatsAll(ctx context.Context, m api.Module, _ uint32, _ uint32, respPtrOut, respLenOut uint32) uint32 {
	p, err := ensureProvider()
	if err != nil {
		return codeProviderUnavail
	}
	stats, err := p.StatsAll(ctx)
	if err != nil {
		return translateDockerErr(err)
	}
	result := make([]statsResponse, len(stats))
	for i := range stats {
		result[i] = containerStatsToResponse(&stats[i])
	}
	return writeMsgpackResponse(ctx, m, result, respPtrOut, respLenOut)
}

func dockerBuild(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
	if !buildMu.TryLock() {
		return codeBuildInProgress
	}

	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		buildMu.Unlock()
		return codeMemoryRead
	}
	var req buildRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		buildMu.Unlock()
		return codeMsgpackDecode
	}
	if req.ImageTag == "" {
		buildMu.Unlock()
		return codeInvalidRequest
	}

	// Path validation: restrict BuildDir to the templates directory.
	if req.BuildDir == "" {
		req.BuildDir = os.Getenv("TEMPLATES_DIR")
		if req.BuildDir == "" {
			req.BuildDir = "/app/templates"
		}
	}
	absDir, err := filepath.Abs(req.BuildDir)
	if err != nil {
		buildMu.Unlock()
		return codeInvalidRequest
	}
	allowedBase := os.Getenv("TEMPLATES_DIR")
	if allowedBase == "" {
		allowedBase = "/app/templates"
	}
	absBase, _ := filepath.Abs(allowedBase)
	if !pathContains(absBase, absDir) {
		buildMu.Unlock()
		return codeInvalidRequest
	}

	buildStateMu.Lock()
	buildState.Building = true
	buildState.LastError = ""
	buildStateMu.Unlock()

	// Detach the build from the caller's request-scoped context — the
	// cell-side host call returns immediately, so ctx is cancelled
	// before the subprocess finishes. The build is tracked via
	// buildState and runs until the docker CLI exits on its own.
	_ = ctx

	go func() {
		defer buildMu.Unlock()
		defer func() {
			buildStateMu.Lock()
			buildState.Building = false
			buildStateMu.Unlock()
		}()

		args := []string{"build"}
		for k, v := range req.BuildArgs {
			args = append(args, "--build-arg", k+"="+v)
		}
		args = append(args, "-t", req.ImageTag, req.BuildDir)

		log.Printf("[pulp-ext-docker] build: docker %s", strings.Join(args, " "))
		cmd := osexec.Command("docker", args...)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		if err := cmd.Run(); err != nil {
			errMsg := fmt.Sprintf("%v: %s", err, out.String())
			buildStateMu.Lock()
			buildState.LastError = errMsg
			buildStateMu.Unlock()
			log.Printf("[pulp-ext-docker] build failed: %s", errMsg)
			return
		}

		now := time.Now().Unix()
		buildStateMu.Lock()
		buildState.LastBuildTime = now
		buildStateMu.Unlock()
		log.Printf("[pulp-ext-docker] build success: %s", req.ImageTag)
	}()

	return codeOK
}

func dockerBuildStatus(ctx context.Context, m api.Module, _ uint32, _ uint32, respPtrOut, respLenOut uint32) uint32 {
	buildStateMu.RLock()
	snapshot := buildState
	buildStateMu.RUnlock()
	return writeMsgpackResponse(ctx, m, snapshot, respPtrOut, respLenOut)
}

// writeMsgpackResponse encodes v → cell memory via pulp_alloc.
func writeMsgpackResponse(ctx context.Context, m api.Module, v any, respPtrOut, respLenOut uint32) uint32 {
	encoded, err := msgpack.Marshal(v)
	if err != nil {
		return codeMsgpackEncode
	}
	allocFn := m.ExportedFunction("pulp_alloc")
	if allocFn == nil {
		return codeAllocFailed
	}
	var ptr uint32
	if len(encoded) > 0 {
		res, err := allocFn.Call(ctx, uint64(len(encoded)))
		if err != nil || len(res) == 0 {
			return codeAllocFailed
		}
		ptr = uint32(res[0])
		if ptr == 0 {
			return codeAllocFailed
		}
		if !m.Memory().Write(ptr, encoded) {
			return codeMemoryWrite
		}
	}
	if !m.Memory().WriteUint32Le(respPtrOut, ptr) {
		return codeMemoryWrite
	}
	if !m.Memory().WriteUint32Le(respLenOut, uint32(len(encoded))) {
		return codeMemoryWrite
	}
	return codeOK
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
