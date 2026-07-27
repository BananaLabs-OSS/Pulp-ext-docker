package dockerext

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/bananalabs-oss/potassium/orchestrator"
)

type fakeHostFleetRuntime struct {
	allocated     []orchestrator.AllocateRequest
	deallocated   []string
	executed      []fakeRuntimeExec
	restarted     []string
	servers       map[string]*orchestrator.Server
	err           error
	getErr        error
	deallocateErr error
	nameOverride  string
	operation     int
	failAfter     int
	running       bool
	regenMarker   bool
	savesEnabled  bool
}

type fakeRuntimeExec struct {
	containerID string
	command     []string
}

func (f *fakeHostFleetRuntime) Get(_ context.Context, id string) (*orchestrator.Server, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.err != nil {
		return nil, f.err
	}
	server := f.servers[id]
	if server == nil {
		return nil, errors.New("not found")
	}
	return server, nil
}

func (f *fakeHostFleetRuntime) Allocate(_ context.Context, request orchestrator.AllocateRequest) (*orchestrator.Server, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.allocated = append(f.allocated, request)
	name := request.Name
	if f.nameOverride != "" {
		name = f.nameOverride
	}
	server := &orchestrator.Server{ID: "container-1", Name: "/" + name, Status: orchestrator.StatusRunning}
	if f.servers == nil {
		f.servers = map[string]*orchestrator.Server{}
	}
	f.servers[server.ID] = server
	return server, nil
}

func (f *fakeHostFleetRuntime) Deallocate(_ context.Context, id string) error {
	if f.deallocateErr != nil {
		return f.deallocateErr
	}
	if f.err != nil {
		return f.err
	}
	f.deallocated = append(f.deallocated, id)
	return nil
}

func (f *fakeHostFleetRuntime) Exec(_ context.Context, containerID string, command []string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.executed = append(f.executed, fakeRuntimeExec{containerID: containerID, command: append([]string(nil), command...)})
	switch {
	case reflect.DeepEqual(command, []string{"rcon", "save-off"}):
		f.savesEnabled = false
	case reflect.DeepEqual(command, []string{"sh", "-c", "touch /data/.sessions-regen"}):
		f.regenMarker = true
	}
	f.operation++
	if f.failAfter == f.operation {
		return "", errors.New("simulated host crash after operation")
	}
	return "", nil
}

func (f *fakeHostFleetRuntime) Restart(_ context.Context, containerID string) error {
	if f.err != nil {
		return f.err
	}
	f.restarted = append(f.restarted, containerID)
	f.running = true
	f.operation++
	if f.failAfter == f.operation {
		return errors.New("simulated host crash after operation")
	}
	return nil
}

func TestHostScopedEffectExecutorUsesOnlyScopedTypedDockerProvision(t *testing.T) {
	scope := mustScope(t, "evolution", "prod-a", "fleet", "worker-1")
	runtime := &fakeHostFleetRuntime{}
	factory := newHostScopedEffectExecutorFactory(func(_ context.Context, got ext.Scope) (HostFleetRuntime, error) {
		if got != scope {
			t.Fatalf("runtime scope = %v, want %v", got, scope)
		}
		return runtime, nil
	})
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatalf("ForScope: %v", err)
	}
	intent, err := effect.NewIntent("provision-1", effect.KindFleetServerProvision, "order-1:provision", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "image": "itzg/minecraft-server:latest",
		"environment": map[string]string{"EULA": "TRUE"},
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	receipt, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("ExecuteIntent: %v", err)
	}
	if err := receipt.ValidateFor(intent); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	if len(runtime.allocated) != 1 {
		t.Fatalf("allocations = %d, want 1", len(runtime.allocated))
	}
	if got, want := runtime.allocated[0].Name, effectContainerName(scope, intent.IdempotencyKey); got != want {
		t.Fatalf("container name = %q, want %q", got, want)
	}
	result, err := effect.DecodeResult[map[string]any](receipt)
	if err != nil {
		t.Fatal(err)
	}
	if result["server_id"] != "server-1" || result["node_id"] != "node-a" || result["container_id"] != "container-1" {
		t.Fatalf("provision receipt result = %#v", result)
	}
}

func TestHostScopedEffectExecutorPreservesApprovedFleetProvisionEnvelope(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	runtime := &fakeHostFleetRuntime{}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	reference := "registry.example.test/sessions@sha256:" + strings.Repeat("a", 64)
	intent, err := effect.NewIntent("provision-approved", effect.KindFleetServerProvision, "order-approved:provision", map[string]any{
		"server_id": "server-approved", "node_id": "node-a",
		"image": map[string]any{
			"reference": reference, "approval_id": "approval-1", "policy_version": "policy-1", "approved": true,
		},
		"resources": map[string]any{"cpu": 2.5, "memory": int64(6 << 30)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err != nil {
		t.Fatalf("ExecuteIntent: %v", err)
	}
	if len(runtime.allocated) != 1 {
		t.Fatalf("allocations = %#v", runtime.allocated)
	}
	request := runtime.allocated[0]
	if request.Image != reference || request.CPULimit != 2.5 || request.MemoryLimit != int64(6<<30) {
		t.Fatalf("approved allocation = %#v", request)
	}
}

func TestHostScopedEffectExecutorFailsClosedForUnavailableOrUnsafeRuntime(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "fleet", "worker-1")
	intent, err := effect.NewIntent("provision-1", effect.KindFleetServerProvision, "order-1:provision", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "image": "paper:latest",
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	unavailable := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return nil, errors.New("docker unavailable")
	})
	executor, err := unavailable.ForScope(scope)
	if err != nil {
		t.Fatalf("ForScope unavailable: %v", err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err == nil {
		t.Fatal("unavailable Docker runtime was accepted")
	}

	unsafe := &fakeHostFleetRuntime{nameOverride: "pulp-other-app-fleet-foreign"}
	unsafeFactory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return unsafe, nil })
	executor, err = unsafeFactory.ForScope(scope)
	if err != nil {
		t.Fatalf("ForScope unsafe: %v", err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err == nil {
		t.Fatal("provider container outside scoped namespace was accepted")
	}
}

func TestHostScopedEffectExecutorUsesContainerIDAndOwnsDeprovisionTarget(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	containerID := "container-owned"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effect.NewIntent("destroy-1", effect.KindFleetServerDeprovision, "server-1:destroy", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "container_id": containerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("ExecuteIntent: %v", err)
	}
	if len(runtime.deallocated) != 1 || runtime.deallocated[0] != containerID {
		t.Fatalf("deallocated = %#v", runtime.deallocated)
	}
	result, err := effect.DecodeResult[map[string]any](receipt)
	if err != nil {
		t.Fatal(err)
	}
	if result["server_id"] != "server-1" || result["node_id"] != "node-a" || result["container_id"] != containerID {
		t.Fatalf("receipt result = %#v", result)
	}
}

func TestHostScopedEffectExecutorTreatsMissingDeprovisionTargetAsReplaySuccess(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	runtime := &fakeHostFleetRuntime{getErr: errors.New("No such container: already-gone")}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effect.NewIntent("destroy-1", effect.KindFleetServerDeprovision, "server-1:destroy", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "container_id": "already-gone",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err != nil {
		t.Fatalf("missing target replay: %v", err)
	}
	if len(runtime.deallocated) != 0 {
		t.Fatalf("missing target deallocated = %#v", runtime.deallocated)
	}
}

func TestHostScopedEffectExecutorTreatsDeallocateNotFoundAsReplaySuccess(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	containerID := "container-raced-away"
	runtime := &fakeHostFleetRuntime{
		servers: map[string]*orchestrator.Server{
			containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
		},
		deallocateErr: errors.New("No such container: raced-away"),
	}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effect.NewIntent("destroy-1", effect.KindFleetServerDeprovision, "server-1:destroy", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "container_id": containerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err != nil {
		t.Fatalf("deallocate not-found replay: %v", err)
	}
}

func TestHostScopedEffectExecutorRejectsDeprovisionOutsideApplicationScope(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	containerID := "container-foreign"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/pulp-evolution-default-sessions-fleet-primary-owned", Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effect.NewIntent("destroy-1", effect.KindFleetServerDeprovision, "server-1:destroy", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "container_id": containerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err == nil {
		t.Fatal("cross-application deprovision target was accepted")
	}
	if len(runtime.deallocated) != 0 {
		t.Fatalf("cross-application deallocated = %#v", runtime.deallocated)
	}
}

func TestHostScopedEffectExecutorExecutesBoundedSaveFlush(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	containerID := "container-rcon"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effect.NewIntent("flush-1", effect.KindFleetExtensionApply, "server-1:flush", map[string]any{
		"extension": "rcon", "server_id": "server-1", "node_id": "node-a",
		"container_id": containerID, "rcon_action": "save_flush",
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("ExecuteIntent: %v", err)
	}
	if len(runtime.executed) != 1 || runtime.executed[0].containerID != containerID ||
		len(runtime.executed[0].command) != 2 || runtime.executed[0].command[0] != "rcon" ||
		runtime.executed[0].command[1] != "save-all flush" {
		t.Fatalf("executed = %#v", runtime.executed)
	}
	result, err := effect.DecodeResult[map[string]any](receipt)
	if err != nil {
		t.Fatal(err)
	}
	if result["server_id"] != "server-1" || result["node_id"] != "node-a" || result["container_id"] != containerID {
		t.Fatalf("receipt result = %#v", result)
	}
}

func TestHostScopedEffectExecutorRejectsRCONOutsideApplicationScope(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	containerID := "container-foreign"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/pulp-other-app-sessions-fleet-primary-owned", Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := effect.NewIntent("flush-1", effect.KindFleetExtensionApply, "server-1:flush", map[string]any{
		"extension": "rcon", "server_id": "server-1", "node_id": "node-a",
		"container_id": containerID, "rcon_action": "save_flush",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err == nil {
		t.Fatal("cross-application RCON target was accepted")
	}
	if len(runtime.executed) != 0 {
		t.Fatalf("cross-application commands = %#v", runtime.executed)
	}
}

func TestHostScopedEffectExecutorRejectsUnboundedRCONCommandSurface(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-b", "sessions-fleet", "primary")
	containerID := "container-rcon"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil })
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	for index, payload := range []map[string]any{
		{"extension": "rcon", "server_id": "server-1", "node_id": "node-a", "container_id": containerID, "rcon_action": "shell", "message": "stop"},
		{"extension": "rcon", "server_id": "server-1", "node_id": "node-a", "container_id": containerID, "rcon_action": "gamerule_set", "rule": "sendCommandFeedback", "value": "true"},
		{"extension": "rcon", "server_id": "server-1", "node_id": "node-a", "container_id": containerID, "rcon_action": "announce", "message": "hello\nstop"},
	} {
		intent, err := effect.NewIntent(fmt.Sprintf("rcon-invalid-%d", index), effect.KindFleetExtensionApply, fmt.Sprintf("rcon-invalid-%d", index), payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executor.ExecuteIntent(context.Background(), scope, intent); err == nil {
			t.Fatalf("unbounded RCON payload %d executed", index)
		}
	}
	if len(runtime.executed) != 0 {
		t.Fatalf("unbounded RCON commands = %#v", runtime.executed)
	}
}
