package dockerext

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/bananalabs-oss/potassium/orchestrator"
	"github.com/vmihailenco/msgpack/v5"
)

// HostFleetRuntime is the deliberately small privileged surface required by
// the canonical Docker fleet adapter. The actual Pulp Docker provider is used
// in production; keeping this interface narrow makes contract tests hermetic.
type HostFleetRuntime interface {
	Get(context.Context, string) (*orchestrator.Server, error)
	Allocate(context.Context, orchestrator.AllocateRequest) (*orchestrator.Server, error)
	Deallocate(context.Context, string) error
}

// HostFleetExecRuntime is the bounded optional surface required by RCON
// effects. Keeping it separate preserves the provision/deprovision runtime
// contract for existing hosts while preventing a generic exec handle from
// leaking beyond the host adapter.
type HostFleetExecRuntime interface {
	Exec(context.Context, string, []string) (string, error)
}

// HostFleetRestartRuntime is the single additional privilege used by the
// typed restart and regenerate operations. The guest never supplies restart
// options; the host always invokes the provider's bounded Restart operation.
type HostFleetRestartRuntime interface {
	Restart(context.Context, string) error
}

type fleetExtensionPayload struct {
	Extension   string `msgpack:"extension"`
	ServerID    string `msgpack:"server_id,omitempty"`
	NodeID      string `msgpack:"node_id,omitempty"`
	ContainerID string `msgpack:"container_id,omitempty"`
	ObjectID    string `msgpack:"object_id,omitempty"`
	ObjectKey   string `msgpack:"object_key,omitempty"`
	Reason      string `msgpack:"reason,omitempty"`
	Limit       int    `msgpack:"limit,omitempty"`
	RCONAction  string `msgpack:"rcon_action,omitempty"`
	Message     string `msgpack:"message,omitempty"`
	Rule        string `msgpack:"rule,omitempty"`
	Value       string `msgpack:"value,omitempty"`
}

type hostFleetRuntimeResolver func(context.Context, ext.Scope) (HostFleetRuntime, error)

// hostFleetEffectExecutors is the one idempotency authority for both the
// deployment outbox dispatcher and the narrow guest capability. Package bytes
// are shared, but ScopedEffectExecutorFactory still partitions receipts by the
// exact Fleet application scope.
var hostFleetEffectExecutors = newHostScopedEffectExecutorFactory(func(ctx context.Context, scope ext.Scope) (HostFleetRuntime, error) {
	return ensureProvider(withDockerScope(ctx, scope))
})

// NewHostScopedEffectExecutorFactory returns the extension-owned canonical
// factory, rather than creating a second replay cache. Callers that dispatch
// asynchronously and guests that invoke effect.fleet.runtime therefore settle
// the same intent exactly once for one Fleet application scope.
func NewHostScopedEffectExecutorFactory() *ScopedEffectExecutorFactory {
	return hostFleetEffectExecutors
}

func newHostScopedEffectExecutorFactory(resolve hostFleetRuntimeResolver) *ScopedEffectExecutorFactory {
	return NewScopedEffectExecutorFactory(func(key ext.ResourceKey) (*EffectExecutor, error) {
		return NewEffectExecutor(&hostFleetBackend{scope: key.Scope(), resolve: resolve})
	})
}

type hostFleetBackend struct {
	scope   ext.Scope
	resolve hostFleetRuntimeResolver
}

func (b *hostFleetBackend) runtime(ctx context.Context) (HostFleetRuntime, error) {
	if b == nil || b.resolve == nil {
		return nil, errors.New("docker effects: host fleet runtime is not configured")
	}
	if err := b.scope.Validate(); err != nil {
		return nil, fmt.Errorf("docker effects: scope: %w", err)
	}
	runtime, err := b.resolve(ctx, b.scope)
	if err != nil {
		return nil, fmt.Errorf("docker effects: host fleet runtime unavailable: %w", err)
	}
	if runtime == nil {
		return nil, errors.New("docker effects: host fleet runtime unavailable")
	}
	return runtime, nil
}

// fleetProvisionPayload is intentionally a closed, typed allowlist. It does
// not expose arbitrary Docker API fields, and mount sources still pass the
// extension's fail-closed bind-root guard before allocation.
type fleetProvisionPayload struct {
	Image          string                     `msgpack:"-"`
	ImageValue     any                        `msgpack:"image"`
	Environment    map[string]string          `msgpack:"environment,omitempty"`
	Volumes        map[string]string          `msgpack:"volumes,omitempty"`
	Ports          []orchestrator.PortBinding `msgpack:"ports,omitempty"`
	Network        string                     `msgpack:"network,omitempty"`
	IP             string                     `msgpack:"ip,omitempty"`
	MemoryLimit    int64                      `msgpack:"memory_limit,omitempty"`
	CPULimit       float64                    `msgpack:"cpu_limit,omitempty"`
	DiskIOReadBps  int64                      `msgpack:"disk_io_read_bps,omitempty"`
	DiskIOWriteBps int64                      `msgpack:"disk_io_write_bps,omitempty"`
	DiskSizeLimit  int64                      `msgpack:"disk_size_limit,omitempty"`
	PidsLimit      int64                      `msgpack:"pids_limit,omitempty"`
	MemorySwap     int64                      `msgpack:"memory_swap,omitempty"`
	Resources      fleetProvisionResources    `msgpack:"resources,omitempty"`
}

type fleetProvisionResources struct {
	CPU    float64 `msgpack:"cpu,omitempty"`
	Memory int64   `msgpack:"memory,omitempty"`
}

type fleetApprovedImage struct {
	Reference     string `msgpack:"reference"`
	ApprovalID    string `msgpack:"approval_id"`
	PolicyVersion string `msgpack:"policy_version"`
	Approved      bool   `msgpack:"approved"`
}

func (b *hostFleetBackend) Provision(ctx context.Context, request ProvisionRequest) (map[string]any, error) {
	if request.Scope != b.scope {
		return nil, errors.New("docker effects: provision scope does not match executor")
	}
	payload, err := decodeFleetProvisionPayload(request.Payload)
	if err != nil {
		return nil, err
	}
	if err := validateVolumes(payload.Volumes); err != nil {
		return nil, fmt.Errorf("docker effects: provision volumes: %w", err)
	}
	runtime, err := b.runtime(ctx)
	if err != nil {
		return nil, err
	}
	server, err := runtime.Allocate(ctx, orchestrator.AllocateRequest{
		Image: payload.Image, Name: request.ContainerName, Environment: payload.Environment,
		Volumes: payload.Volumes, Ports: payload.Ports, Network: payload.Network, IP: payload.IP,
		MemoryLimit: payload.MemoryLimit, CPULimit: payload.CPULimit, DiskIOReadBps: payload.DiskIOReadBps,
		DiskIOWriteBps: payload.DiskIOWriteBps, DiskSizeLimit: payload.DiskSizeLimit,
		PidsLimit: payload.PidsLimit, MemorySwap: payload.MemorySwap,
	})
	if err != nil {
		return nil, err
	}
	if server == nil || strings.TrimSpace(server.ID) == "" || !nameOwnedByScope(server.Name, b.scope) {
		return nil, errors.New("docker effects: provider returned a container outside the scoped namespace")
	}
	return fleetServerResult(request, server), nil
}

func (b *hostFleetBackend) Deprovision(ctx context.Context, request DeprovisionRequest) (map[string]any, error) {
	if request.Scope != b.scope {
		return nil, errors.New("docker effects: deprovision scope does not match executor")
	}
	if strings.TrimSpace(request.ContainerID) == "" {
		return nil, errors.New("docker effects: deprovision container id is required")
	}
	runtime, err := b.runtime(ctx)
	if err != nil {
		return nil, err
	}
	server, err := runtime.Get(ctx, request.ContainerID)
	if err != nil {
		if isNotFound(err) {
			return fleetDeprovisionResult(request), nil
		}
		return nil, err
	}
	if server == nil || strings.TrimSpace(server.ID) == "" || server.ID != request.ContainerID || !nameOwnedByScope(server.Name, b.scope) {
		return nil, errors.New("docker effects: target is outside the scoped namespace")
	}
	if err := runtime.Deallocate(ctx, request.ContainerID); err != nil {
		if !isNotFound(err) {
			return nil, err
		}
	}
	return fleetDeprovisionResult(request), nil
}

// Upload and world deletion require a separate object-storage capability.
// This Docker adapter refuses them instead of silently treating filesystem or
// object-store work as a Docker privilege.
func (*hostFleetBackend) Upload(context.Context, UploadRequest) (map[string]any, error) {
	return nil, errors.New("docker effects: fleet upload is not a Docker host effect")
}

func (*hostFleetBackend) DeleteWorld(context.Context, WorldDeleteRequest) (map[string]any, error) {
	return nil, errors.New("docker effects: fleet world deletion is not a Docker host effect")
}

// ExecuteCanonicalFleetEffect keeps the adapter closed over the canonical
// contract. Docker currently implements provision and deprovision only; the
// remaining canonical kinds fail closed until a specifically privileged host
// adapter is supplied rather than receiving a generic Docker escape hatch.
func (b *hostFleetBackend) ExecuteCanonicalFleetEffect(ctx context.Context, request CanonicalFleetRequest) (map[string]any, error) {
	if request.Scope != b.scope {
		return nil, errors.New("docker effects: canonical fleet scope does not match executor")
	}
	switch request.Kind {
	case effect.KindFleetRuntimeObservationExecute:
		return b.executeRuntimeObservation(ctx, request)
	case effect.KindFleetExtensionApply:
		payload, err := decodeFleetExtensionPayload(request.Payload)
		if err != nil {
			return nil, err
		}
		switch payload.Extension {
		case "rcon":
			return b.executeRCON(ctx, payload)
		case "restart", "regenerate":
			return b.executeRuntimeOperation(ctx, payload)
		default:
			return nil, fmt.Errorf("docker effects: fleet extension %q is not configured", payload.Extension)
		}
	case effect.KindFleetServerReconfigure, effect.KindFleetServerSuspend, effect.KindFleetServerResume:
		return nil, fmt.Errorf("docker effects: canonical fleet effect %q is not configured", request.Kind)
	default:
		return nil, fmt.Errorf("docker effects: unsupported canonical fleet effect %q", request.Kind)
	}
}

func (b *hostFleetBackend) executeRuntimeOperation(ctx context.Context, payload fleetExtensionPayload) (map[string]any, error) {
	if strings.TrimSpace(payload.ServerID) == "" || strings.TrimSpace(payload.NodeID) == "" || strings.TrimSpace(payload.ContainerID) == "" {
		return nil, errors.New("docker effects: runtime operation server, node, and container identity are required")
	}
	runtime, err := b.runtime(ctx)
	if err != nil {
		return nil, err
	}
	server, err := runtime.Get(ctx, payload.ContainerID)
	if err != nil {
		return nil, err
	}
	if server == nil || server.ID != payload.ContainerID || !nameOwnedByScope(server.Name, b.scope) {
		return nil, errors.New("docker effects: runtime operation target is outside the scoped namespace")
	}
	restartRuntime, ok := runtime.(HostFleetRestartRuntime)
	if !ok {
		return nil, errors.New("docker effects: scoped runtime does not support restart")
	}

	status := "restarted"
	if payload.Extension == "regenerate" {
		execRuntime, ok := runtime.(HostFleetExecRuntime)
		if !ok {
			return nil, errors.New("docker effects: scoped runtime does not support regeneration preparation")
		}
		// These are fixed host-owned operations. No command, path, marker, or
		// configuration value is accepted from the effect payload.
		for _, command := range [][]string{
			{"rcon", "save-off"},
			{"rcon", "save-all flush"},
			{"sh", "-c", "touch /data/.sessions-regen"},
		} {
			if _, err := execRuntime.Exec(ctx, payload.ContainerID, command); err != nil {
				return nil, fmt.Errorf("docker effects: prepare world regeneration: %w", err)
			}
		}
		status = "regenerated"
	}
	if err := restartRuntime.Restart(ctx, payload.ContainerID); err != nil {
		return nil, fmt.Errorf("docker effects: %s runtime: %w", payload.Extension, err)
	}
	result := fleetIdentityResult(payload.ServerID, payload.NodeID, payload.ContainerID, status)
	result["operation"] = payload.Extension
	return result, nil
}

func decodeFleetExtensionPayload(payload map[string]any) (fleetExtensionPayload, error) {
	encoded, err := msgpack.Marshal(payload)
	if err != nil {
		return fleetExtensionPayload{}, fmt.Errorf("docker effects: encode extension payload: %w", err)
	}
	var decoded fleetExtensionPayload
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		return fleetExtensionPayload{}, fmt.Errorf("docker effects: invalid typed extension payload: %w", err)
	}
	if strings.TrimSpace(decoded.Extension) == "" {
		return fleetExtensionPayload{}, errors.New("docker effects: fleet extension is required")
	}
	return decoded, nil
}

func (b *hostFleetBackend) executeRCON(ctx context.Context, payload fleetExtensionPayload) (map[string]any, error) {
	if strings.TrimSpace(payload.ServerID) == "" || strings.TrimSpace(payload.NodeID) == "" || strings.TrimSpace(payload.ContainerID) == "" {
		return nil, errors.New("docker effects: RCON server, node, and container identity are required")
	}
	command, err := fleetRCONCommand(payload)
	if err != nil {
		return nil, err
	}
	runtime, err := b.runtime(ctx)
	if err != nil {
		return nil, err
	}
	server, err := runtime.Get(ctx, payload.ContainerID)
	if err != nil {
		return nil, err
	}
	if server == nil || server.ID != payload.ContainerID || !nameOwnedByScope(server.Name, b.scope) {
		return nil, errors.New("docker effects: RCON target is outside the scoped namespace")
	}
	execRuntime, ok := runtime.(HostFleetExecRuntime)
	if !ok {
		return nil, errors.New("docker effects: scoped runtime does not support RCON exec")
	}
	if _, err := execRuntime.Exec(ctx, payload.ContainerID, command); err != nil {
		return nil, err
	}
	return fleetIdentityResult(payload.ServerID, payload.NodeID, payload.ContainerID, "rcon_"+payload.RCONAction), nil
}

var fleetRCONName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

func fleetRCONCommand(payload fleetExtensionPayload) ([]string, error) {
	switch payload.RCONAction {
	case "save_off":
		return []string{"rcon", "save-off"}, nil
	case "save_flush":
		return []string{"rcon", "save-all flush"}, nil
	case "save_on":
		return []string{"rcon", "save-on"}, nil
	case "whitelist_reload":
		return []string{"rcon", "whitelist reload"}, nil
	case "announce":
		if !safeRCONText(payload.Message, 500) {
			return nil, errors.New("docker effects: RCON announcement is invalid")
		}
		return []string{"rcon", "say " + payload.Message}, nil
	case "gamerule_set":
		if !validFleetGameRule(payload.Rule, payload.Value) {
			return nil, errors.New("docker effects: RCON gamerule is invalid")
		}
		return []string{"rcon", "gamerule " + payload.Rule + " " + payload.Value}, nil
	case "difficulty_set":
		switch payload.Value {
		case "peaceful", "easy", "normal", "hard":
			return []string{"rcon", "difficulty " + payload.Value}, nil
		default:
			return nil, errors.New("docker effects: RCON difficulty is invalid")
		}
	case "default_gamemode_set":
		switch payload.Value {
		case "survival", "creative", "adventure", "spectator":
			return []string{"rcon", "defaultgamemode " + payload.Value}, nil
		default:
			return nil, errors.New("docker effects: RCON default game mode is invalid")
		}
	default:
		return nil, fmt.Errorf("docker effects: RCON action %q is not supported", payload.RCONAction)
	}
}

func safeRCONText(value string, limit int) bool {
	if !utf8.ValidString(value) || value == "" || strings.TrimSpace(value) != value {
		return false
	}
	if count := utf8.RuneCountInString(value); count < 1 || count > limit {
		return false
	}
	for _, r := range value {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

var fleetBooleanGameRules = map[string]struct{}{
	"keepInventory": {}, "doDaylightCycle": {}, "doWeatherCycle": {}, "mobGriefing": {},
	"doMobSpawning": {}, "fireDamage": {}, "fallDamage": {}, "drowningDamage": {},
	"naturalRegeneration": {}, "showDeathMessages": {}, "announceAdvancements": {},
	"commandBlockOutput": {}, "doImmediateRespawn": {}, "universalAnger": {},
	"forgiveDeadPlayers": {},
}

var fleetNumericGameRules = map[string]int{
	"randomTickSpeed": 100000, "spawnRadius": 100000, "playersSleepingPercentage": 100000,
}

func validFleetGameRule(rule, value string) bool {
	if !fleetRCONName.MatchString(rule) {
		return false
	}
	if _, ok := fleetBooleanGameRules[rule]; ok {
		return value == "true" || value == "false"
	}
	maximum, ok := fleetNumericGameRules[rule]
	if !ok {
		return false
	}
	parsed, err := strconv.Atoi(value)
	return err == nil && parsed >= 0 && parsed <= maximum
}

func decodeFleetProvisionPayload(payload map[string]any) (fleetProvisionPayload, error) {
	encoded, err := msgpack.Marshal(payload)
	if err != nil {
		return fleetProvisionPayload{}, fmt.Errorf("docker effects: encode provision payload: %w", err)
	}
	var decoded fleetProvisionPayload
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		return fleetProvisionPayload{}, fmt.Errorf("docker effects: invalid typed provision payload: %w", err)
	}
	switch image := decoded.ImageValue.(type) {
	case string:
		decoded.Image = strings.TrimSpace(image)
	default:
		imageWire, err := msgpack.Marshal(image)
		if err != nil {
			return fleetProvisionPayload{}, fmt.Errorf("docker effects: encode approved provision image: %w", err)
		}
		var approved fleetApprovedImage
		if err := msgpack.Unmarshal(imageWire, &approved); err != nil {
			return fleetProvisionPayload{}, fmt.Errorf("docker effects: invalid approved provision image: %w", err)
		}
		if !approved.Approved || !fleetApprovedImageReference.MatchString(approved.Reference) ||
			!fleetRCONName.MatchString(approved.ApprovalID) || !fleetRCONName.MatchString(approved.PolicyVersion) {
			return fleetProvisionPayload{}, errors.New("docker effects: approved provision image envelope is invalid")
		}
		decoded.Image = approved.Reference
	}
	if strings.TrimSpace(decoded.Image) == "" {
		return fleetProvisionPayload{}, errors.New("docker effects: provision image is required")
	}
	if decoded.CPULimit == 0 {
		decoded.CPULimit = decoded.Resources.CPU
	}
	if decoded.MemoryLimit == 0 {
		decoded.MemoryLimit = decoded.Resources.Memory
	}
	return decoded, nil
}

var fleetApprovedImageReference = regexp.MustCompile(`^[a-z0-9][a-z0-9._/:@-]*@sha256:[a-f0-9]{64}$`)

func fleetServerResult(request ProvisionRequest, server *orchestrator.Server) map[string]any {
	return map[string]any{
		"server_id": request.ServerID, "node_id": request.NodeID,
		"container_id": server.ID, "container_name": server.Name,
		"status": string(server.Status), "ip": server.IP, "ports": server.Ports,
		"completed_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func fleetDeprovisionResult(request DeprovisionRequest) map[string]any {
	result := fleetIdentityResult(request.ServerID, request.NodeID, request.ContainerID, "deprovisioned")
	result["deprovisioned"] = true
	return result
}

func fleetIdentityResult(serverID, nodeID, containerID, status string) map[string]any {
	return map[string]any{
		"server_id": serverID, "node_id": nodeID, "container_id": containerID,
		"status": status, "completed_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
}
