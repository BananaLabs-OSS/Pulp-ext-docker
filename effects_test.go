package dockerext

import (
	"context"
	"errors"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
)

type fakeFleetBackend struct {
	provisions   []ProvisionRequest
	deprovisions []DeprovisionRequest
	uploads      []UploadRequest
	worlds       []WorldDeleteRequest
	canonical    []CanonicalFleetRequest
	failNext     error
}

func (f *fakeFleetBackend) Provision(_ context.Context, request ProvisionRequest) (map[string]any, error) {
	if err := f.takeFailure(); err != nil {
		return nil, err
	}
	f.provisions = append(f.provisions, request)
	return map[string]any{"container": request.ContainerName}, nil
}
func (f *fakeFleetBackend) Deprovision(_ context.Context, request DeprovisionRequest) (map[string]any, error) {
	if err := f.takeFailure(); err != nil {
		return nil, err
	}
	f.deprovisions = append(f.deprovisions, request)
	return map[string]any{"server_id": request.ServerID, "node_id": request.NodeID, "container_id": request.ContainerID}, nil
}
func (f *fakeFleetBackend) Upload(_ context.Context, request UploadRequest) (map[string]any, error) {
	if err := f.takeFailure(); err != nil {
		return nil, err
	}
	f.uploads = append(f.uploads, request)
	return map[string]any{"upload_id": request.UploadID}, nil
}
func (f *fakeFleetBackend) DeleteWorld(_ context.Context, request WorldDeleteRequest) (map[string]any, error) {
	if err := f.takeFailure(); err != nil {
		return nil, err
	}
	f.worlds = append(f.worlds, request)
	return map[string]any{"world_id": request.WorldID}, nil
}
func (f *fakeFleetBackend) takeFailure() error { err := f.failNext; f.failNext = nil; return err }

func (f *fakeFleetBackend) ExecuteCanonicalFleetEffect(_ context.Context, request CanonicalFleetRequest) (map[string]any, error) {
	if err := f.takeFailure(); err != nil {
		return nil, err
	}
	f.canonical = append(f.canonical, request)
	return map[string]any{"kind": request.Kind, "server_id": request.Payload["server_id"]}, nil
}

func TestEffectExecutorReplaysReceiptWithoutDuplicateProvision(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	scope := mustScope(t, "sessions", "prod-a", "fleet", "worker-1")
	effect := FleetEffect{ID: "provision-1", Kind: EffectProvision, IdempotencyKey: "order-1:provision", Payload: map[string]any{"server_id": "server-1", "node_id": "node-a", "template": "paper"}}

	first, err := executor.Execute(context.Background(), scope, effect)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	second, err := executor.Execute(context.Background(), scope, effect)
	if err != nil {
		t.Fatalf("replayed Execute: %v", err)
	}
	if len(backend.provisions) != 1 {
		t.Fatalf("provision calls = %d, want 1", len(backend.provisions))
	}
	if first.Result["container"] != second.Result["container"] {
		t.Fatalf("replay result changed: %#v / %#v", first, second)
	}
	if got := backend.provisions[0].ContainerName; got == "" || got != effectContainerName(scope, effect.IdempotencyKey) {
		t.Fatalf("container name = %q", got)
	}
}

func TestEffectExecutorSeparatesSameKeyAcrossApplicationAndCellInstances(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	effect := FleetEffect{ID: "provision-1", Kind: "sessions.fleet.provision.request", IdempotencyKey: "order-1:provision", Payload: map[string]any{"server_id": "server-1", "node_id": "node-a"}}
	for _, scope := range []ext.Scope{
		mustScope(t, "sessions", "prod-a", "fleet", "worker-1"),
		mustScope(t, "sessions", "prod-a", "fleet", "worker-2"),
		mustScope(t, "evolution", "prod-a", "fleet", "worker-1"),
	} {
		if _, err := executor.Execute(context.Background(), scope, effect); err != nil {
			t.Fatalf("Execute: %v", err)
		}
	}
	if len(backend.provisions) != 3 {
		t.Fatalf("provision calls = %d, want 3", len(backend.provisions))
	}
	seen := map[string]bool{}
	for _, request := range backend.provisions {
		if seen[request.ContainerName] {
			t.Fatalf("scope collision for %q", request.ContainerName)
		}
		seen[request.ContainerName] = true
	}
}

func TestEffectExecutorFailedAttemptCanRetry(t *testing.T) {
	backend := &fakeFleetBackend{failNext: errors.New("docker unavailable")}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	scope := mustScope(t, "sessions", "prod-a", "fleet", "worker-1")
	effect := FleetEffect{ID: "deprovision-1", Kind: EffectDeprovision, IdempotencyKey: "server-1:deprovision", Payload: map[string]any{"server_id": "server-1", "container_id": "container-1"}}
	if _, err := executor.Execute(context.Background(), scope, effect); err == nil {
		t.Fatal("failed backend Execute = nil")
	}
	if _, err := executor.Execute(context.Background(), scope, effect); err != nil {
		t.Fatalf("retry Execute: %v", err)
	}
	if len(backend.deprovisions) != 1 {
		t.Fatalf("successful deprovision calls = %d, want 1", len(backend.deprovisions))
	}
}

func TestEffectExecutorRejectsConflictingIdempotencyKey(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	scope := mustScope(t, "sessions", "prod-a", "fleet", "worker-1")
	first := FleetEffect{ID: "deprovision-1", Kind: EffectDeprovision, IdempotencyKey: "server-1", Payload: map[string]any{"server_id": "server-1", "container_id": "container-1"}}
	if _, err := executor.Execute(context.Background(), scope, first); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	conflict := FleetEffect{ID: "deprovision-2", Kind: EffectDeprovision, IdempotencyKey: first.IdempotencyKey, Payload: map[string]any{"server_id": "server-2", "container_id": "container-2"}}
	if _, err := executor.Execute(context.Background(), scope, conflict); err == nil {
		t.Fatal("conflicting idempotency key accepted")
	}
	if len(backend.deprovisions) != 1 {
		t.Fatalf("deprovision calls = %d, want 1", len(backend.deprovisions))
	}
}

func TestEffectExecutorDispatchesUploadAndWorldDeletion(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	scope := mustScope(t, "sessions", "prod-a", "fleet", "worker-1")
	for _, effect := range []FleetEffect{
		{ID: "upload-1", Kind: EffectUpload, IdempotencyKey: "upload-1", Payload: map[string]any{"upload_id": "upload-1", "server_id": "server-1", "object_key": "uploads/a.zip"}},
		{ID: "world-1", Kind: EffectWorldDelete, IdempotencyKey: "world-1", Payload: map[string]any{"world_id": "world-1", "server_id": "server-1", "object_key": "worlds/a.zip"}},
	} {
		if _, err := executor.Execute(context.Background(), scope, effect); err != nil {
			t.Fatalf("Execute(%s): %v", effect.Kind, err)
		}
	}
	if len(backend.uploads) != 1 || len(backend.worlds) != 1 {
		t.Fatalf("upload/world calls = %d/%d, want 1/1", len(backend.uploads), len(backend.worlds))
	}
}

type fakeScopedCell struct {
	name  string
	scope ext.Scope
}

func (c fakeScopedCell) Name() string     { return c.name }
func (c fakeScopedCell) Scope() ext.Scope { return c.scope }

type fakeLegacyCell struct{ name string }

func (c fakeLegacyCell) Name() string { return c.name }

func TestExecuteForCellUsesScopeThenLegacyFallback(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	effect := FleetEffect{ID: "deprovision-1", Kind: EffectDeprovision, IdempotencyKey: "server-1", Payload: map[string]any{"server_id": "server-1", "container_id": "container-1"}}
	scoped := fakeScopedCell{name: "fleet", scope: mustScope(t, "sessions", "prod-a", "fleet", "worker-1")}
	if _, err := executor.ExecuteForCell(context.Background(), scoped, effect); err != nil {
		t.Fatalf("scoped ExecuteForCell: %v", err)
	}
	if _, err := executor.ExecuteForCell(context.Background(), fakeLegacyCell{name: "fleet"}, effect); err != nil {
		t.Fatalf("legacy ExecuteForCell: %v", err)
	}
	if len(backend.deprovisions) != 2 {
		t.Fatalf("deprovision calls = %d, want 2", len(backend.deprovisions))
	}
	if got := backend.deprovisions[1].Scope.ApplicationID(); got != "legacy" {
		t.Fatalf("legacy scope app = %q, want legacy", got)
	}
}

func TestEffectExecutorExecutesCanonicalIntentAndNormalizesLegacyAlias(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	intent, err := effect.NewIntent("provision-1", "fleet.provision", "order-1:provision", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "template": "paper",
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	scope := mustScope(t, "sessions", "prod-a", "fleet", "worker-1")
	first, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("ExecuteIntent: %v", err)
	}
	second, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("replayed ExecuteIntent: %v", err)
	}
	if err := first.ValidateFor(intent); err != nil {
		t.Fatalf("first receipt: %v", err)
	}
	if first.Status != effect.Completed || second.Status != effect.Completed {
		t.Fatalf("receipt statuses = %q/%q, want completed", first.Status, second.Status)
	}
	if first.Kind != effect.KindFleetServerProvision {
		t.Fatalf("canonical receipt kind = %q", first.Kind)
	}
	if len(backend.provisions) != 1 {
		t.Fatalf("provision calls = %d, want 1", len(backend.provisions))
	}
}

func TestEffectExecutorDispatchesCanonicalFleetOperationsInScope(t *testing.T) {
	backend := &fakeFleetBackend{}
	executor, err := NewEffectExecutor(backend)
	if err != nil {
		t.Fatalf("NewEffectExecutor: %v", err)
	}
	intent, err := effect.NewIntent("reconfigure-1", effect.KindFleetServerReconfigure, "server-1:reconfigure", map[string]any{
		"server_id": "server-1", "cpu": int64(2),
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	scope := mustScope(t, "evolution", "prod-a", "fleet", "worker-2")
	receipt, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("ExecuteIntent: %v", err)
	}
	if err := receipt.ValidateFor(intent); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	if len(backend.canonical) != 1 {
		t.Fatalf("canonical backend calls = %d, want 1", len(backend.canonical))
	}
	request := backend.canonical[0]
	if request.Scope != scope || request.Kind != effect.KindFleetServerReconfigure || request.IdempotencyKey != intent.IdempotencyKey {
		t.Fatalf("canonical request = %#v", request)
	}
}

func TestScopedEffectExecutorFactorySeparatesAppsAndTearsDownOneApp(t *testing.T) {
	backend := &fakeFleetBackend{}
	factory := NewScopedEffectExecutorFactory(func(_ ext.ResourceKey) (*EffectExecutor, error) {
		return NewEffectExecutor(backend)
	})
	appA := mustScope(t, "evolution", "a", "fleet", "primary")
	appB := mustScope(t, "sessions", "b", "fleet", "primary")
	executorA, err := factory.ForScope(appA)
	if err != nil {
		t.Fatalf("executor A: %v", err)
	}
	executorB, err := factory.ForScope(appB)
	if err != nil {
		t.Fatalf("executor B: %v", err)
	}
	if executorA == executorB || factory.Count() != 2 {
		t.Fatalf("executors are not application scoped: %#v / %#v (count %d)", executorA, executorB, factory.Count())
	}
	if err := factory.TeardownScope(mustScope(t, "evolution", "a", "host", "primary")); err != nil {
		t.Fatalf("teardown app A: %v", err)
	}
	if factory.Count() != 1 {
		t.Fatalf("executors after app A teardown = %d, want 1", factory.Count())
	}
	stillB, err := factory.ForScope(appB)
	if err != nil || stillB != executorB {
		t.Fatalf("app B executor was not retained: %p / %p, %v", stillB, executorB, err)
	}
}

func mustScope(t *testing.T, app, appInstance, cell, cellInstance string) ext.Scope {
	t.Helper()
	scope, err := ext.NewScope(app, appInstance, cell, cellInstance)
	if err != nil {
		t.Fatalf("NewScope: %v", err)
	}
	return scope
}
