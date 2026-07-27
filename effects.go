package dockerext

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
)

// Fleet effect kinds are deliberately host-owned. A state cell emits one of
// these durable intents; it never receives the Docker socket or a provider
// handle. The aliases keep the executor compatible with the first Sessions
// fleet-cell contracts while the public wire settles on one spelling.
const (
	// The public defaults are Fiber's canonical, versioned host-effect kinds.
	// New state owners must emit them through effect.Intent.
	EffectProvision          = effect.KindFleetServerProvision
	EffectReconfigure        = effect.KindFleetServerReconfigure
	EffectSuspend            = effect.KindFleetServerSuspend
	EffectResume             = effect.KindFleetServerResume
	EffectDeprovision        = effect.KindFleetServerDeprovision
	EffectExtension          = effect.KindFleetExtensionApply
	EffectRuntimeObservation = effect.KindFleetRuntimeObservationExecute

	// CanonicalEffect* remain source-compatible names for callers that adopted
	// the first contract-adaptation branch before its public constants settled.
	CanonicalEffectProvision   = EffectProvision
	CanonicalEffectReconfigure = EffectReconfigure
	CanonicalEffectSuspend     = EffectSuspend
	CanonicalEffectResume      = EffectResume
	CanonicalEffectDeprovision = EffectDeprovision
	CanonicalEffectExtension   = EffectExtension

	// These legacy aliases remain accepted by Execute and NormalizeKind so
	// existing persisted outbox records drain into canonical receipts.
	LegacyEffectProvision   = "fleet.provision"
	LegacyEffectDeprovision = "fleet.deprovision"
	EffectUpload            = "fleet.upload"
	EffectWorldDelete       = "fleet.world.delete"
)

// FleetEffect is the transport-neutral subset of a durable fleet outbox
// record. ID is a diagnostic handle; IdempotencyKey is the stable identity of
// the requested privileged action and must be retained across every retry.
type FleetEffect struct {
	ID             string         `msgpack:"id"`
	Kind           string         `msgpack:"kind"`
	IdempotencyKey string         `msgpack:"idempotency_key"`
	Payload        map[string]any `msgpack:"payload"`
}

// FleetEffectReceipt is the host acknowledgement which callers may persist
// back into their durable outbox. Completed receipts are replay-safe: the
// executor returns the original receipt without calling the backend again.
type FleetEffectReceipt struct {
	ID             string         `msgpack:"id"`
	Kind           string         `msgpack:"kind"`
	IdempotencyKey string         `msgpack:"idempotency_key"`
	Result         map[string]any `msgpack:"result,omitempty"`
}

// FleetBackend is the only boundary at which privileged work is allowed. A
// deployment supplies an adapter backed by Docker, R2, or another host-owned
// provider. WASM and Lua only ever create FleetEffect values.
//
// Implementations must make an individual call safe to repeat after an
// uncertain process failure. In particular provisioning should use the
// deterministic name supplied in ProvisionRequest.
type FleetBackend interface {
	Provision(context.Context, ProvisionRequest) (map[string]any, error)
	Deprovision(context.Context, DeprovisionRequest) (map[string]any, error)
	Upload(context.Context, UploadRequest) (map[string]any, error)
	DeleteWorld(context.Context, WorldDeleteRequest) (map[string]any, error)
}

// ProvisionRequest is the sanitized host request for a fleet.provision
// effect. ContainerName is deterministic for the application/cell placement
// and effect idempotency key; a Docker adapter must use it as the provider
// idempotency boundary rather than accepting a cell-selected global name.
type ProvisionRequest struct {
	Scope         ext.Scope
	ServerID      string
	NodeID        string
	Template      string
	ContainerName string
	Payload       map[string]any
}

type DeprovisionRequest struct {
	Scope       ext.Scope
	ServerID    string
	NodeID      string
	ContainerID string
	Payload     map[string]any
}

type UploadRequest struct {
	Scope     ext.Scope
	UploadID  string
	ServerID  string
	ObjectKey string
	Payload   map[string]any
}

type WorldDeleteRequest struct {
	Scope     ext.Scope
	WorldID   string
	ServerID  string
	ObjectKey string
	Payload   map[string]any
}

// CanonicalFleetRequest is the generic host-owned operation surface for the
// canonical fleet kinds that do not map to the original provision and
// deprovision backend methods. A Docker deployment may implement this optional
// interface to support reconfigure, suspend, resume, and extension apply
// without exposing its provider client to a WASM cell.
type CanonicalFleetRequest struct {
	Scope          ext.Scope
	Kind           string
	IdempotencyKey string
	Payload        map[string]any
}

// CanonicalFleetBackend is optional so existing Docker adapters retain their
// original provision/deprovision contract. Hosts that enable the additional
// canonical fleet operations own the privileged implementation here.
type CanonicalFleetBackend interface {
	ExecuteCanonicalFleetEffect(context.Context, CanonicalFleetRequest) (map[string]any, error)
}

// EffectExecutor turns durable fleet intents into host-side operations. Its
// receipt namespace includes every application and cell instance, so the same
// shared WASM bytes can safely run in multiple Pulp applications and in more
// than one placement of a cell in an application.
type EffectExecutor struct {
	backend FleetBackend

	mu       sync.Mutex
	receipts map[ext.ResourceKey]effectReceiptRecord
}

type effectReceiptRecord struct {
	receipt FleetEffectReceipt
	id      string
	kind    string
	payload map[string]any
}

func NewEffectExecutor(backend FleetBackend) (*EffectExecutor, error) {
	if backend == nil {
		return nil, errors.New("docker effects: backend is required")
	}
	return &EffectExecutor{backend: backend, receipts: make(map[ext.ResourceKey]effectReceiptRecord)}, nil
}

// ScopedEffectExecutorFactory owns one mutable executor per application/cell
// placement. It is the production wiring boundary for hosts whose FleetBackend
// carries client state: a shared extension binary may reuse code, but never an
// idempotency cache or backend handle across two Pulp applications.
type ScopedEffectExecutorFactory struct {
	mu        sync.Mutex
	executors map[ext.ResourceKey]*EffectExecutor
	new       func(ext.ResourceKey) (*EffectExecutor, error)
}

func NewScopedEffectExecutorFactory(newExecutor func(ext.ResourceKey) (*EffectExecutor, error)) *ScopedEffectExecutorFactory {
	return &ScopedEffectExecutorFactory{
		executors: make(map[ext.ResourceKey]*EffectExecutor),
		new:       newExecutor,
	}
}

func (f *ScopedEffectExecutorFactory) ForScope(scope ext.Scope) (*EffectExecutor, error) {
	if f == nil || f.new == nil {
		return nil, errors.New("docker effects: scoped executor factory is not configured")
	}
	key, err := scope.ResourceKey("fleet-effect", "executor")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if executor := f.executors[key]; executor != nil {
		return executor, nil
	}
	executor, err := f.new(key)
	if err != nil {
		return nil, err
	}
	if executor == nil {
		return nil, errors.New("docker effects: scoped executor factory returned nil executor")
	}
	f.executors[key] = executor
	return executor, nil
}

func (f *ScopedEffectExecutorFactory) ForCell(cell ext.Cell) (*EffectExecutor, error) {
	scope, err := ext.ValidatedScopeOf(cell)
	if err != nil {
		return nil, err
	}
	return f.ForScope(scope)
}

// TeardownScope drops all executor state belonging to one application instance.
// It deliberately matches application identity rather than requiring callers
// to know every cell placement created during a rolling update.
func (f *ScopedEffectExecutorFactory) TeardownScope(scope ext.Scope) error {
	if f == nil {
		return nil
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.executors {
		owner := key.Scope()
		if owner.ApplicationID() == scope.ApplicationID() && owner.ApplicationInstanceID() == scope.ApplicationInstanceID() {
			delete(f.executors, key)
		}
	}
	return nil
}

func (f *ScopedEffectExecutorFactory) Count() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.executors)
}

// Execute applies one pending effect exactly once per live scope and stable
// idempotency key. Failed backend attempts are deliberately not receipts: the
// durable outbox can retry them, while completed calls replay their original
// acknowledgement. The mutex also closes the duplicate-delivery race within a
// host process.
func (e *EffectExecutor) Execute(ctx context.Context, scope ext.Scope, effect FleetEffect) (FleetEffectReceipt, error) {
	if e == nil || e.backend == nil {
		return FleetEffectReceipt{}, errors.New("docker effects: executor is not configured")
	}
	if err := scope.Validate(); err != nil {
		return FleetEffectReceipt{}, fmt.Errorf("docker effects: invalid scope: %w", err)
	}
	if err := effect.validate(); err != nil {
		return FleetEffectReceipt{}, err
	}
	key, err := scope.ResourceKey("fleet-effect", effect.IdempotencyKey)
	if err != nil {
		return FleetEffectReceipt{}, fmt.Errorf("docker effects: receipt key: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if record, ok := e.receipts[key]; ok {
		if record.id != effect.ID || record.kind != canonicalEffectKind(effect.Kind) || !reflect.DeepEqual(record.payload, effect.Payload) {
			return FleetEffectReceipt{}, fmt.Errorf("docker effects: idempotency key %q belongs to a different effect", effect.IdempotencyKey)
		}
		return copyReceipt(record.receipt), nil
	}

	result, err := e.apply(ctx, scope, effect)
	if err != nil {
		return FleetEffectReceipt{}, err
	}
	receipt := FleetEffectReceipt{
		ID: effect.ID, Kind: canonicalEffectKind(effect.Kind), IdempotencyKey: effect.IdempotencyKey,
		Result: copyPayload(result),
	}
	e.receipts[key] = effectReceiptRecord{receipt: receipt, id: effect.ID, kind: receipt.Kind, payload: copyPayload(effect.Payload)}
	return copyReceipt(receipt), nil
}

// ExecuteForCell selects the application/cell instance placement owned by the
// Pulp host. ext.ScopeOf retains its stable legacy fallback for older Cell
// implementations, so a shared extension binary supports both host versions.
func (e *EffectExecutor) ExecuteForCell(ctx context.Context, cell ext.Cell, effect FleetEffect) (FleetEffectReceipt, error) {
	return e.Execute(ctx, ext.ScopeOf(cell), effect)
}

// ExecuteIntent is the canonical v1 host-effect boundary. It accepts only the
// versioned Fiber envelope (including its supported legacy aliases), executes
// inside the supplied application/cell scope, and returns a receipt bound to
// the exact durable intent. Legacy callers can keep using Execute until their
// persisted outboxes have converged on effect.Intent.
func (e *EffectExecutor) ExecuteIntent(ctx context.Context, scope ext.Scope, intent effect.Intent) (effect.Receipt, error) {
	if err := intent.Normalize(); err != nil {
		return effect.Receipt{}, fmt.Errorf("docker effects: normalize intent: %w", err)
	}
	payload, err := effect.DecodePayload[map[string]any](intent)
	if err != nil {
		return effect.Receipt{}, fmt.Errorf("docker effects: decode intent payload: %w", err)
	}

	legacy := FleetEffect{
		ID: intent.ID, Kind: intent.Kind, IdempotencyKey: intent.IdempotencyKey, Payload: payload,
	}
	receipt, err := e.Execute(ctx, scope, legacy)
	if err != nil {
		return effect.Receipt{}, err
	}
	canonical, err := effect.NewCompletedReceipt(intent, receipt.Result)
	if err != nil {
		return effect.Receipt{}, fmt.Errorf("docker effects: build receipt: %w", err)
	}
	return canonical, nil
}

// ExecuteIntentForCell obtains the explicit scoped cell placement before
// executing a canonical effect. ScopeOf retains the established legacy-cell
// fallback without allowing one explicit application scope to leak into
// another.
func (e *EffectExecutor) ExecuteIntentForCell(ctx context.Context, cell ext.Cell, intent effect.Intent) (effect.Receipt, error) {
	return e.ExecuteIntent(ctx, ext.ScopeOf(cell), intent)
}

func (e *EffectExecutor) apply(ctx context.Context, scope ext.Scope, effect FleetEffect) (map[string]any, error) {
	payload := copyPayload(effect.Payload)
	switch canonicalEffectKind(effect.Kind) {
	case CanonicalEffectProvision:
		serverID, err := requiredPayloadString(payload, "server_id")
		if err != nil {
			return nil, err
		}
		nodeID, err := requiredPayloadString(payload, "node_id")
		if err != nil {
			return nil, err
		}
		return e.backend.Provision(ctx, ProvisionRequest{
			Scope: scope, ServerID: serverID, NodeID: nodeID, Template: optionalPayloadString(payload, "template"),
			ContainerName: effectContainerName(scope, effect.IdempotencyKey), Payload: payload,
		})
	case CanonicalEffectDeprovision:
		serverID, err := requiredPayloadString(payload, "server_id")
		if err != nil {
			return nil, err
		}
		containerID, err := requiredPayloadString(payload, "container_id")
		if err != nil {
			return nil, err
		}
		return e.backend.Deprovision(ctx, DeprovisionRequest{
			Scope: scope, ServerID: serverID, NodeID: optionalPayloadString(payload, "node_id"),
			ContainerID: containerID, Payload: payload,
		})
	case CanonicalEffectReconfigure, CanonicalEffectSuspend, CanonicalEffectResume, CanonicalEffectExtension, EffectRuntimeObservation:
		backend, ok := e.backend.(CanonicalFleetBackend)
		if !ok {
			return nil, fmt.Errorf("docker effects: canonical fleet effect %q is not configured", canonicalEffectKind(effect.Kind))
		}
		return backend.ExecuteCanonicalFleetEffect(ctx, CanonicalFleetRequest{
			Scope: scope, Kind: canonicalEffectKind(effect.Kind), IdempotencyKey: effect.IdempotencyKey, Payload: payload,
		})
	case EffectUpload:
		uploadID, err := requiredPayloadString(payload, "upload_id")
		if err != nil {
			return nil, err
		}
		serverID, err := requiredPayloadString(payload, "server_id")
		if err != nil {
			return nil, err
		}
		objectKey, err := requiredPayloadString(payload, "object_key")
		if err != nil {
			return nil, err
		}
		return e.backend.Upload(ctx, UploadRequest{Scope: scope, UploadID: uploadID, ServerID: serverID, ObjectKey: objectKey, Payload: payload})
	case EffectWorldDelete:
		worldID, err := requiredPayloadString(payload, "world_id")
		if err != nil {
			return nil, err
		}
		objectKey, err := requiredPayloadString(payload, "object_key")
		if err != nil {
			return nil, err
		}
		return e.backend.DeleteWorld(ctx, WorldDeleteRequest{Scope: scope, WorldID: worldID, ServerID: optionalPayloadString(payload, "server_id"), ObjectKey: objectKey, Payload: payload})
	default:
		return nil, fmt.Errorf("docker effects: unsupported fleet effect kind %q", effect.Kind)
	}
}

func (effect FleetEffect) validate() error {
	for _, field := range []struct{ name, value string }{{"id", effect.ID}, {"kind", effect.Kind}, {"idempotency key", effect.IdempotencyKey}} {
		if strings.TrimSpace(field.value) == "" || strings.ContainsRune(field.value, '\x00') {
			return fmt.Errorf("docker effects: %s is required", field.name)
		}
	}
	if len(effect.Payload) == 0 {
		return errors.New("docker effects: payload is required")
	}
	return nil
}

func canonicalEffectKind(kind string) string {
	if canonical, err := effect.NormalizeKind(kind); err == nil {
		return canonical
	}
	switch kind {
	case "sessions.fleet.provision.request":
		return EffectProvision
	case "sessions.fleet.upload.request":
		return EffectUpload
	case "sessions.fleet.world.delete":
		return EffectWorldDelete
	default:
		return kind
	}
}

func requiredPayloadString(payload map[string]any, field string) (string, error) {
	value := optionalPayloadString(payload, field)
	if value == "" {
		return "", fmt.Errorf("docker effects: payload %q is required", field)
	}
	return value, nil
}

func optionalPayloadString(payload map[string]any, field string) string {
	value, _ := payload[field].(string)
	return strings.TrimSpace(value)
}

func effectContainerName(scope ext.Scope, idempotencyKey string) string {
	parts := []string{scope.ApplicationID(), scope.ApplicationInstanceID(), scope.CellID(), scope.CellInstanceID(), idempotencyKey}
	for i := range parts {
		parts[i] = sanitizeCellID(parts[i])
	}
	return "pulp-" + strings.Join(parts, "-")
}

func copyPayload(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func copyReceipt(receipt FleetEffectReceipt) FleetEffectReceipt {
	receipt.Result = copyPayload(receipt.Result)
	return receipt
}
