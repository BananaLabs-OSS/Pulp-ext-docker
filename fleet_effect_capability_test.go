package dockerext

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/bananalabs-oss/potassium/orchestrator"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

func TestFleetEffectCapabilityBindsActiveAndStubABI(t *testing.T) {
	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)
	defer runtime.Close(ctx)
	scope := mustScope(t, "sessions", "prod-a", "effects", "primary")
	cell := fakeScopedCell{name: "effects", scope: scope}
	capability := newFleetEffectCapability(NewScopedEffectExecutorFactory(func(key ext.ResourceKey) (*EffectExecutor, error) {
		return NewEffectExecutor(&hostFleetBackend{scope: key.Scope(), resolve: func(context.Context, ext.Scope) (HostFleetRuntime, error) {
			return &fakeHostFleetRuntime{}, nil
		}})
	}))
	active := runtime.NewHostModuleBuilder("fleet_effect_active")
	if err := capability.Register(active, cell); err != nil {
		t.Fatalf("register active: %v", err)
	}
	activeModule, err := active.Instantiate(ctx)
	if err != nil {
		t.Fatalf("instantiate active: %v", err)
	}
	activeDefinition, ok := activeModule.ExportedFunctionDefinitions()[fleetEffectExecuteExport]
	if !ok {
		t.Fatalf("active capability did not export %q", fleetEffectExecuteExport)
	}
	if got, want := activeDefinition.ParamTypes(), []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}; !reflect.DeepEqual(got, want) || !reflect.DeepEqual(activeDefinition.ResultTypes(), []api.ValueType{api.ValueTypeI32}) {
		t.Fatalf("active ABI = params %#v results %#v", got, activeDefinition.ResultTypes())
	}

	stub := runtime.NewHostModuleBuilder("fleet_effect_stub")
	if err := capability.Stub(stub, cell); err != nil {
		t.Fatalf("register stub: %v", err)
	}
	stubModule, err := stub.Instantiate(ctx)
	if err != nil {
		t.Fatalf("instantiate stub: %v", err)
	}
	stubDefinition, ok := stubModule.ExportedFunctionDefinitions()[fleetEffectExecuteExport]
	if !ok {
		t.Fatalf("stub capability did not export %q", fleetEffectExecuteExport)
	}
	if got, want := stubDefinition.ParamTypes(), []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}; !reflect.DeepEqual(got, want) || !reflect.DeepEqual(stubDefinition.ResultTypes(), []api.ValueType{api.ValueTypeI32}) {
		t.Fatalf("stub ABI = params %#v results %#v", got, stubDefinition.ResultTypes())
	}
	if got := fleetEffectExecuteStub(ctx, nil, 0, 0, 0, 0); got != codeCapabilityStubbed {
		t.Fatalf("stub code = %d, want %d", got, codeCapabilityStubbed)
	}
}

func TestFleetEffectCapabilityDerivesSameApplicationFleetPrimaryScope(t *testing.T) {
	caller := mustScope(t, "sessions", "prod-a", "effects", "primary")
	got, err := fleetRuntimeScope(caller)
	if err != nil {
		t.Fatalf("fleetRuntimeScope: %v", err)
	}
	want := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	if got != want {
		t.Fatalf("fleet runtime scope = %#v, want %#v", got, want)
	}
	other := mustScope(t, "evolution", "prod-a", "effects", "primary")
	otherFleet, err := fleetRuntimeScope(other)
	if err != nil {
		t.Fatalf("other fleetRuntimeScope: %v", err)
	}
	if otherFleet == got || otherFleet.ApplicationID() != "evolution" || otherFleet.ApplicationInstanceID() != "prod-a" {
		t.Fatalf("cross-application scope was not isolated: %#v", otherFleet)
	}
}

func TestFleetEffectCapabilityTeardownReleasesDerivedFleetScopeOnly(t *testing.T) {
	resetDockerScopesForTest(t)
	caller := mustScope(t, "sessions", "prod-a", "effects", "primary")
	otherCaller := mustScope(t, "evolution", "prod-a", "effects", "primary")
	fleetScope, err := fleetRuntimeScope(caller)
	if err != nil {
		t.Fatalf("sessions fleetRuntimeScope: %v", err)
	}
	otherFleetScope, err := fleetRuntimeScope(otherCaller)
	if err != nil {
		t.Fatalf("Evolution fleetRuntimeScope: %v", err)
	}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return &fakeHostFleetRuntime{}, nil
	})
	first, err := factory.ForScope(fleetScope)
	if err != nil {
		t.Fatalf("sessions executor: %v", err)
	}
	other, err := factory.ForScope(otherFleetScope)
	if err != nil {
		t.Fatalf("Evolution executor: %v", err)
	}
	if factory.Count() != 2 {
		t.Fatalf("factory count before teardown = %d, want 2", factory.Count())
	}
	firstDockerState, err := dockerStateForScope(fleetScope)
	if err != nil {
		t.Fatalf("sessions Docker state: %v", err)
	}
	otherDockerState, err := dockerStateForScope(otherFleetScope)
	if err != nil {
		t.Fatalf("Evolution Docker state: %v", err)
	}
	capability := newFleetEffectCapability(factory)
	if err := capability.TeardownScope(context.Background(), caller); err != nil {
		t.Fatalf("teardown caller scope: %v", err)
	}
	if factory.Count() != 1 {
		t.Fatalf("factory count after sessions restart = %d, want 1", factory.Count())
	}
	if got := dockerScopeStateCount(); got != 1 {
		t.Fatalf("Docker state count after sessions restart = %d, want 1", got)
	}
	restartedDockerState, err := dockerStateForScope(fleetScope)
	if err != nil {
		t.Fatalf("restarted sessions Docker state: %v", err)
	}
	if restartedDockerState == firstDockerState {
		t.Fatal("sessions restart reused stale Docker provider state")
	}
	stillOtherDockerState, err := dockerStateForScope(otherFleetScope)
	if err != nil {
		t.Fatalf("Evolution Docker state after sessions teardown: %v", err)
	}
	if stillOtherDockerState != otherDockerState {
		t.Fatal("sessions teardown disturbed a different application Docker provider state")
	}
	restarted, err := factory.ForScope(fleetScope)
	if err != nil {
		t.Fatalf("restarted sessions executor: %v", err)
	}
	if restarted == first {
		t.Fatal("sessions restart reused stale fleet executor")
	}
	stillOther, err := factory.ForScope(otherFleetScope)
	if err != nil {
		t.Fatalf("Evolution executor after sessions teardown: %v", err)
	}
	if stillOther != other {
		t.Fatal("sessions teardown disturbed a different application fleet executor")
	}
}

func TestFleetEffectCapabilityRejectsKindsOutsideNarrowAllowlist(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return &fakeHostFleetRuntime{}, nil
	})
	intent, err := effect.NewIntent("provision-1", effect.KindFleetServerProvision, "order-1:provision", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "image": "itzg/minecraft-server:latest",
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	wire, err := effect.MarshalIntent(intent)
	if err != nil {
		t.Fatalf("MarshalIntent: %v", err)
	}
	if _, code := executeFleetEffectWire(context.Background(), factory, scope, wire); code != codeInvalidRequest {
		t.Fatalf("provision code = %d, want %d", code, codeInvalidRequest)
	}
}

func TestFleetEffectCapabilityExecutesSaveFlushAndDeprovisionWithReplay(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-1"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	var resolved []ext.Scope
	factory := newHostScopedEffectExecutorFactory(func(_ context.Context, got ext.Scope) (HostFleetRuntime, error) {
		resolved = append(resolved, got)
		return runtime, nil
	})

	saveFlush, err := effect.NewIntent("flush-1", effect.KindFleetExtensionApply, "server-1:flush", map[string]any{
		"extension": "rcon", "server_id": "server-1", "node_id": "node-a", "container_id": containerID, "rcon_action": "save_flush",
	})
	if err != nil {
		t.Fatalf("NewIntent save flush: %v", err)
	}
	flushWire, err := effect.MarshalIntent(saveFlush)
	if err != nil {
		t.Fatalf("MarshalIntent save flush: %v", err)
	}
	flushResponse, code := executeFleetEffectWire(context.Background(), factory, scope, flushWire)
	if code != codeOK {
		t.Fatalf("save flush code = %d", code)
	}
	flushReceipt, err := effect.UnmarshalReceipt(flushResponse)
	if err != nil {
		t.Fatalf("UnmarshalReceipt save flush: %v", err)
	}
	if flushReceipt.Kind != effect.KindFleetExtensionApply || len(runtime.executed) != 1 || !reflect.DeepEqual(runtime.executed[0].command, []string{"rcon", "save-all flush"}) {
		t.Fatalf("save flush receipt/runtime = %#v / %#v", flushReceipt, runtime.executed)
	}

	deprovision, err := effect.NewIntent("deprovision-1", effect.KindFleetServerDeprovision, "server-1:deprovision", map[string]any{
		"server_id": "server-1", "node_id": "node-a", "container_id": containerID,
	})
	if err != nil {
		t.Fatalf("NewIntent deprovision: %v", err)
	}
	deprovisionWire, err := effect.MarshalIntent(deprovision)
	if err != nil {
		t.Fatalf("MarshalIntent deprovision: %v", err)
	}
	firstWire, firstCode := executeFleetEffectWire(context.Background(), factory, scope, deprovisionWire)
	secondWire, secondCode := executeFleetEffectWire(context.Background(), factory, scope, deprovisionWire)
	if firstCode != codeOK || secondCode != codeOK {
		t.Fatalf("deprovision codes = %d, %d", firstCode, secondCode)
	}
	firstReceipt, err := effect.UnmarshalReceipt(firstWire)
	if err != nil {
		t.Fatalf("UnmarshalReceipt first deprovision: %v", err)
	}
	secondReceipt, err := effect.UnmarshalReceipt(secondWire)
	if err != nil {
		t.Fatalf("UnmarshalReceipt replayed deprovision: %v", err)
	}
	firstResult, err := effect.DecodeResult[map[string]any](firstReceipt)
	if err != nil {
		t.Fatalf("decode first deprovision result: %v", err)
	}
	secondResult, err := effect.DecodeResult[map[string]any](secondReceipt)
	if err != nil {
		t.Fatalf("decode replayed deprovision result: %v", err)
	}
	if firstReceipt.IntentID != secondReceipt.IntentID || firstReceipt.Kind != secondReceipt.Kind || firstReceipt.IdempotencyKey != secondReceipt.IdempotencyKey || firstReceipt.Status != secondReceipt.Status || !reflect.DeepEqual(firstResult, secondResult) || len(runtime.deallocated) != 1 || runtime.deallocated[0] != containerID {
		t.Fatalf("deprovision replayed receipt/result = %#v / %#v, deallocated = %#v", firstReceipt, secondReceipt, runtime.deallocated)
	}
	if len(resolved) != 2 || resolved[0] != scope || resolved[1] != scope {
		t.Fatalf("resolved scopes = %#v, want fleet primary only", resolved)
	}
}

func TestFleetEffectCapabilityExecutesAuditedAnnouncementsWithReplay(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-announce"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(_ context.Context, got ext.Scope) (HostFleetRuntime, error) {
		if got != scope {
			t.Fatalf("resolved scope = %#v, want %#v", got, scope)
		}
		return runtime, nil
	})

	tests := []struct {
		name          string
		id            string
		reason        string
		includeReason bool
		message       string
	}{
		{name: "legacy instant extension without reason", id: "announce-instant", message: fleetInstantExtensionAnnouncement},
		{name: "legacy instant extension with empty reason", id: "announce-instant-empty", includeReason: true, message: fleetInstantExtensionAnnouncement},
		{name: "expiry warning", id: "announce-expiry", reason: "expiry-warning", includeReason: true, message: "Server expires in 10 minutes."},
		{name: "scheduled restart warning", id: "announce-restart", reason: "scheduled-restart-warning", includeReason: true, message: "Server restart in 5 minutes."},
		{name: "five hundred unicode characters", id: "announce-unicode-boundary", reason: "expiry-warning", includeReason: true, message: strings.Repeat("界", 500)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := map[string]any{
				"extension": "rcon", "server_id": "server-announce", "node_id": "node-a",
				"container_id": containerID, "rcon_action": "announce", "message": test.message,
			}
			if test.includeReason {
				payload["reason"] = test.reason
			}
			intent, err := effect.NewIntent(test.id, effect.KindFleetExtensionApply, test.id+":key", payload)
			if err != nil {
				t.Fatalf("NewIntent: %v", err)
			}
			wire, err := effect.MarshalIntent(intent)
			if err != nil {
				t.Fatalf("MarshalIntent: %v", err)
			}
			before := len(runtime.executed)
			firstWire, firstCode := executeFleetEffectWire(context.Background(), factory, scope, wire)
			replayWire, replayCode := executeFleetEffectWire(context.Background(), factory, scope, wire)
			if firstCode != codeOK || replayCode != codeOK {
				t.Fatalf("announcement codes = %d, %d", firstCode, replayCode)
			}
			if len(runtime.executed) != before+1 {
				t.Fatalf("announcement executions = %d, want %d", len(runtime.executed), before+1)
			}
			executed := runtime.executed[before]
			if executed.containerID != containerID || !reflect.DeepEqual(executed.command, []string{"rcon", "say " + test.message}) {
				t.Fatalf("announcement exec = %#v", executed)
			}
			firstReceipt, err := effect.UnmarshalReceipt(firstWire)
			if err != nil {
				t.Fatalf("UnmarshalReceipt first: %v", err)
			}
			replayReceipt, err := effect.UnmarshalReceipt(replayWire)
			if err != nil {
				t.Fatalf("UnmarshalReceipt replay: %v", err)
			}
			firstResult, err := effect.DecodeResult[map[string]any](firstReceipt)
			if err != nil {
				t.Fatalf("DecodeResult first: %v", err)
			}
			replayResult, err := effect.DecodeResult[map[string]any](replayReceipt)
			if err != nil {
				t.Fatalf("DecodeResult replay: %v", err)
			}
			if firstResult["status"] != "rcon_announce" ||
				firstResult["server_id"] != "server-announce" ||
				firstResult["node_id"] != "node-a" ||
				firstResult["container_id"] != containerID ||
				!reflect.DeepEqual(firstResult, replayResult) {
				t.Fatalf("announcement results = %#v / %#v", firstResult, replayResult)
			}
		})
	}
}

func TestFleetEffectCapabilityRejectsUnauditedAnnouncements(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-announce"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return runtime, nil
	})
	base := map[string]any{
		"extension": "rcon", "server_id": "server-announce", "node_id": "node-a",
		"container_id": containerID, "rcon_action": "announce",
		"reason": "expiry-warning", "message": "Server expires in 10 minutes.",
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing reason for generic message", mutate: func(payload map[string]any) { delete(payload, "reason") }},
		{name: "unknown reason", mutate: func(payload map[string]any) { payload["reason"] = "operator-message" }},
		{name: "wrong legacy message", mutate: func(payload map[string]any) {
			payload["reason"] = ""
			payload["message"] = "Server extended."
		}},
		{name: "empty message", mutate: func(payload map[string]any) { payload["message"] = "" }},
		{name: "oversize message", mutate: func(payload map[string]any) { payload["message"] = strings.Repeat("界", 501) }},
		{name: "untrimmed message", mutate: func(payload map[string]any) { payload["message"] = " warning " }},
		{name: "newline", mutate: func(payload map[string]any) { payload["message"] = "hello\nstop" }},
		{name: "control character", mutate: func(payload map[string]any) { payload["message"] = "hello\tstop" }},
		{name: "arbitrary command", mutate: func(payload map[string]any) { payload["command"] = "stop" }},
		{name: "arbitrary command alias", mutate: func(payload map[string]any) { payload["cmd"] = "stop" }},
		{name: "unrelated rule", mutate: func(payload map[string]any) { payload["rule"] = "keepInventory" }},
		{name: "missing server identity", mutate: func(payload map[string]any) { delete(payload, "server_id") }},
		{name: "missing node identity", mutate: func(payload map[string]any) { delete(payload, "node_id") }},
		{name: "missing container identity", mutate: func(payload map[string]any) { delete(payload, "container_id") }},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := make(map[string]any, len(base))
			for key, value := range base {
				payload[key] = value
			}
			test.mutate(payload)
			intent, err := effect.NewIntent(
				fmt.Sprintf("announce-invalid-%d", index),
				effect.KindFleetExtensionApply,
				fmt.Sprintf("announce-invalid-%d:key", index),
				payload,
			)
			if err != nil {
				t.Fatalf("NewIntent: %v", err)
			}
			wire, err := effect.MarshalIntent(intent)
			if err != nil {
				t.Fatalf("MarshalIntent: %v", err)
			}
			if _, code := executeFleetEffectWire(context.Background(), factory, scope, wire); code != codeInvalidRequest {
				t.Fatalf("invalid announcement code = %d, want %d", code, codeInvalidRequest)
			}
		})
	}
	if len(runtime.executed) != 0 {
		t.Fatalf("invalid announcements reached runtime: %#v", runtime.executed)
	}
}

func TestFleetEffectCapabilityRejectsAnnouncementOutsideRuntimeScope(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-other"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(mustScope(t, "evolution", "prod-a", "fleet", "primary")), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return runtime, nil
	})
	intent, err := effect.NewIntent("announce-cross-scope", effect.KindFleetExtensionApply, "announce-cross-scope:key", map[string]any{
		"extension": "rcon", "server_id": "server-other", "node_id": "node-other",
		"container_id": containerID, "rcon_action": "announce",
		"reason": "expiry-warning", "message": "Server expires in 10 minutes.",
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	wire, err := effect.MarshalIntent(intent)
	if err != nil {
		t.Fatalf("MarshalIntent: %v", err)
	}
	if _, code := executeFleetEffectWire(context.Background(), factory, scope, wire); code != codeDockerError {
		t.Fatalf("cross-scope announcement code = %d, want %d", code, codeDockerError)
	}
	if len(runtime.executed) != 0 {
		t.Fatalf("cross-scope announcement reached runtime: %#v", runtime.executed)
	}
}

func TestFleetEffectCapabilityExecutesExactRestartAndRegenerateV1(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-runtime-op"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}, savesEnabled: true}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return runtime, nil
	})

	for _, operation := range []string{"restart", "regenerate"} {
		t.Run(operation, func(t *testing.T) {
			beforeExecs, beforeRestarts := len(runtime.executed), len(runtime.restarted)
			intent, err := effect.NewIntent("runtime-"+operation, effect.KindFleetExtensionApply, "runtime-"+operation+":key", map[string]any{
				"extension": operation, "server_id": "server-runtime", "node_id": "node-runtime",
				"container_id": containerID, "reason": "operator requested",
			})
			if err != nil {
				t.Fatalf("NewIntent: %v", err)
			}
			wire, err := effect.MarshalIntent(intent)
			if err != nil {
				t.Fatalf("MarshalIntent: %v", err)
			}
			response, code := executeFleetEffectWire(context.Background(), factory, scope, wire)
			if code != codeOK {
				t.Fatalf("%s code = %d", operation, code)
			}
			receipt, err := effect.UnmarshalReceipt(response)
			if err != nil {
				t.Fatalf("UnmarshalReceipt: %v", err)
			}
			result, err := effect.DecodeResult[map[string]any](receipt)
			if err != nil {
				t.Fatalf("DecodeResult: %v", err)
			}
			wantStatus := operation + "ed"
			if operation == "regenerate" {
				wantStatus = "regenerated"
			}
			if len(result) != 6 ||
				result["server_id"] != "server-runtime" ||
				result["node_id"] != "node-runtime" ||
				result["container_id"] != containerID ||
				result["operation"] != operation ||
				result["status"] != wantStatus {
				t.Fatalf("%s result = %#v", operation, result)
			}
			completedAt, ok := result["completed_at"].(string)
			if !ok {
				t.Fatalf("%s completed_at = %#v", operation, result["completed_at"])
			}
			if _, err := time.Parse(time.RFC3339Nano, completedAt); err != nil {
				t.Fatalf("%s completed_at %q: %v", operation, completedAt, err)
			}
			if len(runtime.restarted) != beforeRestarts+1 || runtime.restarted[beforeRestarts] != containerID {
				t.Fatalf("%s restarts = %#v", operation, runtime.restarted)
			}
			if operation == "restart" {
				if len(runtime.executed) != beforeExecs {
					t.Fatalf("restart executed commands: %#v", runtime.executed[beforeExecs:])
				}
				return
			}
			wantCommands := [][]string{
				{"rcon", "save-off"},
				{"rcon", "save-all flush"},
				{"sh", "-c", "touch /data/.sessions-regen"},
			}
			if got := runtime.executed[beforeExecs:]; len(got) != len(wantCommands) {
				t.Fatalf("regenerate command count = %d, want %d", len(got), len(wantCommands))
			} else {
				for index, command := range wantCommands {
					if got[index].containerID != containerID || !reflect.DeepEqual(got[index].command, command) {
						t.Fatalf("regenerate command %d = %#v, want %#v", index, got[index], command)
					}
				}
			}
		})
	}
}

func TestFleetEffectCapabilityRejectsRuntimeOperationEscapeFieldsAndScope(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-runtime-op"
	runtime := &fakeHostFleetRuntime{servers: map[string]*orchestrator.Server{
		containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
	}}
	factory := newHostScopedEffectExecutorFactory(func(context.Context, ext.Scope) (HostFleetRuntime, error) {
		return runtime, nil
	})
	base := map[string]any{
		"extension": "regenerate", "server_id": "server-runtime", "node_id": "node-runtime",
		"container_id": containerID, "reason": "operator requested",
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "command", mutate: func(payload map[string]any) { payload["command"] = []string{"sh", "-c", "rm -rf /"} }},
		{name: "path", mutate: func(payload map[string]any) { payload["path"] = "/data/other" }},
		{name: "marker", mutate: func(payload map[string]any) { payload["marker"] = "/tmp/other" }},
		{name: "config", mutate: func(payload map[string]any) { payload["config"] = map[string]string{"seed": "different"} }},
		{name: "rcon action", mutate: func(payload map[string]any) { payload["rcon_action"] = "stop" }},
		{name: "missing server", mutate: func(payload map[string]any) { delete(payload, "server_id") }},
		{name: "missing node", mutate: func(payload map[string]any) { delete(payload, "node_id") }},
		{name: "missing container", mutate: func(payload map[string]any) { delete(payload, "container_id") }},
		{name: "untrimmed reason", mutate: func(payload map[string]any) { payload["reason"] = " operator " }},
		{name: "control reason", mutate: func(payload map[string]any) { payload["reason"] = "operator\nrequested" }},
		{name: "257-byte multibyte reason", mutate: func(payload map[string]any) { payload["reason"] = strings.Repeat("界", 85) + "ab" }},
		{name: "control identity", mutate: func(payload map[string]any) { payload["server_id"] = "server\u0000runtime" }},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := make(map[string]any, len(base))
			for key, value := range base {
				payload[key] = value
			}
			test.mutate(payload)
			intent, err := effect.NewIntent(fmt.Sprintf("runtime-invalid-%d", index), effect.KindFleetExtensionApply, fmt.Sprintf("runtime-invalid-%d:key", index), payload)
			if err != nil {
				t.Fatalf("NewIntent: %v", err)
			}
			wire, err := effect.MarshalIntent(intent)
			if err != nil {
				t.Fatalf("MarshalIntent: %v", err)
			}
			if _, code := executeFleetEffectWire(context.Background(), factory, scope, wire); code != codeInvalidRequest {
				t.Fatalf("invalid runtime operation code = %d, want %d", code, codeInvalidRequest)
			}
		})
	}

	otherScope := mustScope(t, "evolution", "prod-a", "fleet", "primary")
	runtime.servers[containerID].Name = "/" + scopePrefix(otherScope)
	intent, err := effect.NewIntent("runtime-cross-scope", effect.KindFleetExtensionApply, "runtime-cross-scope:key", base)
	if err != nil {
		t.Fatalf("NewIntent cross-scope: %v", err)
	}
	wire, err := effect.MarshalIntent(intent)
	if err != nil {
		t.Fatalf("MarshalIntent cross-scope: %v", err)
	}
	if _, code := executeFleetEffectWire(context.Background(), factory, scope, wire); code != codeDockerError {
		t.Fatalf("cross-scope runtime operation code = %d, want %d", code, codeDockerError)
	}
	if len(runtime.executed) != 0 || len(runtime.restarted) != 0 {
		t.Fatalf("rejected runtime operations reached provider: exec=%#v restart=%#v", runtime.executed, runtime.restarted)
	}
}

func TestFleetRuntimeLifecycleReasonUsesFiberByteBoundary(t *testing.T) {
	payload := map[string]any{
		"extension": "restart", "server_id": "server-runtime", "node_id": "node-runtime",
		"container_id": "container-runtime",
	}
	decoded := fleetExtensionPayload{
		Extension: "restart", ServerID: "server-runtime", NodeID: "node-runtime", ContainerID: "container-runtime",
	}

	accepted := strings.Repeat("界", 85) + "a"
	if len(accepted) != 256 {
		t.Fatalf("accepted fixture bytes = %d, want 256", len(accepted))
	}
	payload["reason"] = accepted
	decoded.Reason = accepted
	if err := validateFleetRuntimeOperation(payload, decoded); err != nil {
		t.Fatalf("256-byte multibyte reason rejected: %v", err)
	}

	rejected := strings.Repeat("界", 85) + "ab"
	if len(rejected) != 257 {
		t.Fatalf("rejected fixture bytes = %d, want 257", len(rejected))
	}
	payload["reason"] = rejected
	decoded.Reason = rejected
	if err := validateFleetRuntimeOperation(payload, decoded); err == nil {
		t.Fatal("257-byte multibyte reason accepted")
	}
}

func TestFleetEffectRegenerateCrashReplayConvergesWithEmptyReceiptCache(t *testing.T) {
	scope := mustScope(t, "sessions", "prod-a", "fleet", "primary")
	containerID := "container-regenerate-crash"
	intent, err := effect.NewIntent("regenerate-crash", effect.KindFleetExtensionApply, "regenerate-crash:key", map[string]any{
		"extension": "regenerate", "server_id": "server-regenerate", "node_id": "node-regenerate", "container_id": containerID,
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	wire, err := effect.MarshalIntent(intent)
	if err != nil {
		t.Fatalf("MarshalIntent: %v", err)
	}
	wantReplayCommands := [][]string{
		{"rcon", "save-off"},
		{"rcon", "save-all flush"},
		{"sh", "-c", "touch /data/.sessions-regen"},
	}

	// Each injected failure occurs after the privileged operation changed the
	// runtime but before a receipt could be cached. A new factory models a host
	// process restart with an empty in-memory receipt cache. The fixed sequence
	// may repeat, including Restart, but it converges to the same bounded state;
	// this is operational idempotency, not an exactly-once claim.
	for failAfter := 1; failAfter <= 4; failAfter++ {
		t.Run(fmt.Sprintf("crash-after-step-%d", failAfter), func(t *testing.T) {
			runtime := &fakeHostFleetRuntime{
				servers: map[string]*orchestrator.Server{
					containerID: {ID: containerID, Name: "/" + scopePrefix(scope), Status: orchestrator.StatusRunning},
				},
				failAfter: failAfter, savesEnabled: true,
			}
			resolve := func(context.Context, ext.Scope) (HostFleetRuntime, error) { return runtime, nil }
			beforeCrash := newHostScopedEffectExecutorFactory(resolve)
			if _, code := executeFleetEffectWire(context.Background(), beforeCrash, scope, wire); code != codeDockerError {
				t.Fatalf("crash execution code = %d, want %d", code, codeDockerError)
			}

			runtime.failAfter = 0
			afterCrash := newHostScopedEffectExecutorFactory(resolve)
			response, code := executeFleetEffectWire(context.Background(), afterCrash, scope, wire)
			if code != codeOK {
				t.Fatalf("replayed execution code = %d", code)
			}
			receipt, err := effect.UnmarshalReceipt(response)
			if err != nil {
				t.Fatalf("UnmarshalReceipt replay: %v", err)
			}
			result, err := effect.DecodeResult[map[string]any](receipt)
			if err != nil {
				t.Fatalf("DecodeResult replay: %v", err)
			}
			if result["operation"] != "regenerate" || result["status"] != "regenerated" {
				t.Fatalf("replayed regenerate result = %#v", result)
			}
			if !runtime.running || !runtime.regenMarker || runtime.savesEnabled {
				t.Fatalf("replayed final state = running:%t marker:%t saves:%t", runtime.running, runtime.regenMarker, runtime.savesEnabled)
			}
			if runtime.operation != failAfter+4 {
				t.Fatalf("operation count = %d, want bounded %d", runtime.operation, failAfter+4)
			}
			prefixCount := failAfter
			if prefixCount > len(wantReplayCommands) {
				prefixCount = len(wantReplayCommands)
			}
			wantAllCommands := append([][]string(nil), wantReplayCommands[:prefixCount]...)
			wantAllCommands = append(wantAllCommands, wantReplayCommands...)
			if len(runtime.executed) != len(wantAllCommands) {
				t.Fatalf("all replay commands = %#v, want %#v", runtime.executed, wantAllCommands)
			}
			for index, command := range wantAllCommands {
				if !reflect.DeepEqual(runtime.executed[index].command, command) || runtime.executed[index].containerID != containerID {
					t.Fatalf("all replay command %d = %#v, want %#v", index, runtime.executed[index], command)
				}
			}
			wantRestarts := 1
			if failAfter == 4 {
				wantRestarts = 2
			}
			if len(runtime.restarted) != wantRestarts {
				t.Fatalf("restart count = %d, want %d", len(runtime.restarted), wantRestarts)
			}
		})
	}
}
