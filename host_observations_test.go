package dockerext

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/bananalabs-oss/potassium/orchestrator"
	"github.com/vmihailenco/msgpack/v5"
)

type observationTestRuntime struct {
	servers map[string]*orchestrator.Server
	outputs map[string]string
	execs   []fakeRuntimeExec
}

func (r *observationTestRuntime) Get(_ context.Context, id string) (*orchestrator.Server, error) {
	server := r.servers[id]
	if server == nil {
		return nil, errors.New("No such container: " + id)
	}
	copy := *server
	return &copy, nil
}

func (*observationTestRuntime) Allocate(context.Context, orchestrator.AllocateRequest) (*orchestrator.Server, error) {
	return nil, errors.New("unexpected observation allocation")
}

func (*observationTestRuntime) Deallocate(context.Context, string) error {
	return errors.New("unexpected observation deallocation")
}

func (r *observationTestRuntime) Exec(_ context.Context, id string, command []string) (string, error) {
	r.execs = append(r.execs, fakeRuntimeExec{containerID: id, command: append([]string(nil), command...)})
	return r.outputs[strings.Join(command, "\x00")], nil
}

func observationTestScope(t *testing.T) ext.Scope {
	t.Helper()
	return mustScope(t, "bananagine", "prod-a", "fleet", "primary")
}

func observationTestRuntimeForScope(t *testing.T, scope ext.Scope) *observationTestRuntime {
	t.Helper()
	return &observationTestRuntime{
		servers: map[string]*orchestrator.Server{
			"container-1": {ID: "container-1", Name: "/" + scopePrefix(scope) + "server-1", Status: orchestrator.StatusRunning},
		},
		outputs: map[string]string{
			strings.Join([]string{"sh", "-c", fleetObservationSettingsCommand}, "\x00"):      "difficulty=hard\nunknown=hidden\n",
			strings.Join([]string{"sh", "-c", fleetObservationPlayerHistoryCommand}, "\x00"): `[{"uuid":"12345678-1234-1234-1234-123456789abc","name":"Steve","expiresOn":"2026-08-26 01:00:00 +0000"}]`,
			strings.Join([]string{"sh", "-c", fleetObservationGameRulesCommand()}, "\x00"):   "RULE:keepInventory:The value of keepInventory is: true\n",
			strings.Join([]string{"rcon", "list"}, "\x00"):                                   "There are 2 of a max of 20 players online: Bob, Alice",
			strings.Join([]string{"sh", "-c", fleetObservationWhitelistCommand}, "\x00"):     `[{"uuid":"12345678-1234-1234-1234-123456789abc","name":"Steve"}]`,
			strings.Join([]string{"sh", "-c", fleetObservationOpsCommand}, "\x00"):           `[{"uuid":"abcdefab-cdef-abcd-efab-cdefabcdefab","name":"Alex","level":4,"bypassesPlayerLimit":true}]`,
			strings.Join([]string{"sh", "-c", fleetObservationBansCommand}, "\x00"):          `[{"uuid":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","name":"Griefer","created":"2026-07-26 01:00:00 +0000","source":"Server","expires":"forever","reason":"Banned by an operator."}]`,
			strings.Join([]string{"sh", "-c", fleetObservationDatapacksCommand}, "\x00"):     "adventure.zip\nquality.ZIP\n",
			strings.Join([]string{"sh", "-c", fleetObservationModsCommand}, "\x00"):          "fabric-api.jar\nvoicechat.jar\n",
		},
	}
}

func observationTestIntent(t *testing.T, field effect.FleetRuntimeObservationFieldV1, key string) effect.Intent {
	t.Helper()
	revision := "fleet-live-v1:" + strings.Repeat("a", 64)
	intent, err := effect.NewIntent("observe-"+key, effect.KindFleetRuntimeObservationExecute, key, effect.FleetRuntimeObservationIntentV1{
		Contract: effect.FleetRuntimeObservationContractV1, ServerID: "server-1", NodeID: "node-1",
		ContainerID: "container-1", Field: field, Generation: revision, SourceRevision: revision,
	})
	if err != nil {
		t.Fatalf("NewIntent: %v", err)
	}
	return intent
}

func observationTestExecutor(t *testing.T, scope ext.Scope, runtime HostFleetRuntime) *EffectExecutor {
	t.Helper()
	factory := newHostScopedEffectExecutorFactory(func(_ context.Context, got ext.Scope) (HostFleetRuntime, error) {
		if got != scope {
			t.Fatalf("resolved scope = %#v, want %#v", got, scope)
		}
		return runtime, nil
	})
	executor, err := factory.ForScope(scope)
	if err != nil {
		t.Fatalf("ForScope: %v", err)
	}
	return executor
}

func TestHostScopedRuntimeObservationExecutesEightFixedTypedFields(t *testing.T) {
	fields := []effect.FleetRuntimeObservationFieldV1{
		effect.FleetRuntimeObservationFieldSettingsV1,
		effect.FleetRuntimeObservationFieldGameRulesV1,
		effect.FleetRuntimeObservationFieldPlayersV1,
		effect.FleetRuntimeObservationFieldPlayerHistoryV1,
		effect.FleetRuntimeObservationFieldAccessV1,
		effect.FleetRuntimeObservationFieldAccessSnapshotV1,
		effect.FleetRuntimeObservationFieldArtifactsV1,
		effect.FleetRuntimeObservationFieldStatusV1,
	}
	for _, field := range fields {
		t.Run(string(field), func(t *testing.T) {
			scope := observationTestScope(t)
			runtime := observationTestRuntimeForScope(t, scope)
			executor := observationTestExecutor(t, scope, runtime)
			intent := observationTestIntent(t, field, "observe:"+string(field))
			receipt, err := executor.ExecuteIntent(context.Background(), scope, intent)
			if err != nil {
				t.Fatalf("ExecuteIntent: %v", err)
			}
			result, err := effect.DecodeResult[effect.FleetRuntimeObservationReceiptV1](receipt)
			if err != nil {
				t.Fatalf("DecodeResult: %v", err)
			}
			payload, err := effect.DecodePayload[effect.FleetRuntimeObservationIntentV1](intent)
			if err != nil {
				t.Fatal(err)
			}
			if err := result.ValidateFor(payload); err != nil {
				t.Fatalf("ValidateFor: %v", err)
			}
			switch field {
			case effect.FleetRuntimeObservationFieldSettingsV1:
				if !reflect.DeepEqual(result.Data.Settings, map[string]string{"difficulty": "hard"}) {
					t.Fatalf("settings = %#v", result.Data.Settings)
				}
			case effect.FleetRuntimeObservationFieldGameRulesV1:
				if result.Data.GameRules["keepInventory"] != "true" {
					t.Fatalf("gamerules = %#v", result.Data.GameRules)
				}
			case effect.FleetRuntimeObservationFieldPlayersV1:
				if !reflect.DeepEqual(result.Data.Players, []string{"Alice", "Bob"}) {
					t.Fatalf("players = %#v", result.Data.Players)
				}
			case effect.FleetRuntimeObservationFieldPlayerHistoryV1:
				want := []effect.FleetRuntimePlayerHistoryEntryV1{{UUID: "12345678-1234-1234-1234-123456789abc", Name: "Steve", ExpiresOn: "2026-08-26 01:00:00 +0000"}}
				if !reflect.DeepEqual(result.Data.PlayerHistory, want) {
					t.Fatalf("player history = %#v", result.Data.PlayerHistory)
				}
			case effect.FleetRuntimeObservationFieldAccessV1:
				if result.Data.Access == nil || !reflect.DeepEqual(result.Data.Access.Whitelist, []string{"Steve"}) || !reflect.DeepEqual(result.Data.Access.Operators, []string{"Alex"}) || !reflect.DeepEqual(result.Data.Access.Bans, []string{"Griefer"}) {
					t.Fatalf("access = %#v", result.Data.Access)
				}
			case effect.FleetRuntimeObservationFieldAccessSnapshotV1:
				if result.Data.AccessSnapshot == nil || len(result.Data.AccessSnapshot.Whitelist) != 1 || result.Data.AccessSnapshot.Whitelist[0].UUID == "" || len(result.Data.AccessSnapshot.Operators) != 1 || result.Data.AccessSnapshot.Operators[0].Level != 4 || !result.Data.AccessSnapshot.Operators[0].BypassesPlayerLimit || len(result.Data.AccessSnapshot.Bans) != 1 || result.Data.AccessSnapshot.Bans[0].Reason != "Banned by an operator." {
					t.Fatalf("access snapshot = %#v", result.Data.AccessSnapshot)
				}
			case effect.FleetRuntimeObservationFieldArtifactsV1:
				want := []effect.FleetRuntimeObservedArtifactV1{{Name: "adventure.zip", Kind: "datapack"}, {Name: "quality.ZIP", Kind: "datapack"}, {Name: "fabric-api.jar", Kind: "mod"}, {Name: "voicechat.jar", Kind: "mod"}}
				if !reflect.DeepEqual(result.Data.Artifacts, want) {
					t.Fatalf("artifacts = %#v", result.Data.Artifacts)
				}
			case effect.FleetRuntimeObservationFieldStatusV1:
				if result.Data.Status != "running" {
					t.Fatalf("status = %q", result.Data.Status)
				}
			}
		})
	}
}

func TestHostScopedRuntimeObservationRejectsCrossScopeAndControlInjection(t *testing.T) {
	scope := observationTestScope(t)
	runtime := observationTestRuntimeForScope(t, scope)
	runtime.servers["container-1"].Name = "/" + scopePrefix(mustScope(t, "other", "prod-a", "fleet", "primary")) + "server-1"
	executor := observationTestExecutor(t, scope, runtime)
	if _, err := executor.ExecuteIntent(context.Background(), scope, observationTestIntent(t, effect.FleetRuntimeObservationFieldPlayersV1, "observe:foreign")); err == nil {
		t.Fatal("cross-scope observation succeeded")
	}
	if len(runtime.execs) != 0 {
		t.Fatalf("cross-scope observation executed %#v", runtime.execs)
	}

	revision := "fleet-live-v1:" + strings.Repeat("a", 64)
	for _, forbidden := range []string{"command", "path", "query"} {
		payload, err := msgpack.Marshal(map[string]any{
			"contract": effect.FleetRuntimeObservationContractV1, "server_id": "server-1", "node_id": "node-1",
			"container_id": "container-1", "field": "players", "generation": revision, "source_revision": revision,
			forbidden: "attacker-controlled",
		})
		if err != nil {
			t.Fatal(err)
		}
		injected := effect.Intent{Version: effect.VersionV1, ID: "observe-injected-" + forbidden, Kind: effect.KindFleetRuntimeObservationExecute, IdempotencyKey: "observe:injected:" + forbidden, Payload: payload}
		if _, err := executor.ExecuteIntent(context.Background(), scope, injected); err == nil {
			t.Fatalf("caller-controlled observation %s validated", forbidden)
		}
	}
}

func TestHostScopedRuntimeObservationBoundsAndReplays(t *testing.T) {
	scope := observationTestScope(t)
	runtime := observationTestRuntimeForScope(t, scope)
	executor := observationTestExecutor(t, scope, runtime)
	intent := observationTestIntent(t, effect.FleetRuntimeObservationFieldSettingsV1, "observe:settings:replay")
	first, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("first ExecuteIntent: %v", err)
	}
	second, err := executor.ExecuteIntent(context.Background(), scope, intent)
	if err != nil {
		t.Fatalf("replayed ExecuteIntent: %v", err)
	}
	firstResult, err := effect.DecodeResult[effect.FleetRuntimeObservationReceiptV1](first)
	if err != nil {
		t.Fatalf("decode first replay result: %v", err)
	}
	secondResult, err := effect.DecodeResult[effect.FleetRuntimeObservationReceiptV1](second)
	if err != nil {
		t.Fatalf("decode second replay result: %v", err)
	}
	if len(runtime.execs) != 1 || first.IntentID != second.IntentID || first.IdempotencyKey != second.IdempotencyKey || !reflect.DeepEqual(firstResult, secondResult) {
		t.Fatalf("replay receipts/execs = %#v / %#v / %#v", first, second, runtime.execs)
	}

	oversizeRuntime := observationTestRuntimeForScope(t, scope)
	oversizeRuntime.outputs[strings.Join([]string{"sh", "-c", fleetObservationSettingsCommand}, "\x00")] = strings.Repeat("x", fleetObservationMaxBytes+1)
	oversizeExecutor := observationTestExecutor(t, scope, oversizeRuntime)
	if _, err := oversizeExecutor.ExecuteIntent(context.Background(), scope, observationTestIntent(t, effect.FleetRuntimeObservationFieldSettingsV1, "observe:settings:oversize")); err == nil {
		t.Fatal("oversize observation succeeded")
	}
}

func TestHostScopedRuntimeObservationDetailedReadsStaySeparateFromPublicProjections(t *testing.T) {
	scope := observationTestScope(t)
	runtime := observationTestRuntimeForScope(t, scope)
	executor := observationTestExecutor(t, scope, runtime)
	execute := func(field effect.FleetRuntimeObservationFieldV1, key string) effect.FleetRuntimeObservationReceiptV1 {
		t.Helper()
		receipt, err := executor.ExecuteIntent(context.Background(), scope, observationTestIntent(t, field, key))
		if err != nil {
			t.Fatal(err)
		}
		result, err := effect.DecodeResult[effect.FleetRuntimeObservationReceiptV1](receipt)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	players := execute(effect.FleetRuntimeObservationFieldPlayersV1, "observe:players:separate")
	history := execute(effect.FleetRuntimeObservationFieldPlayerHistoryV1, "observe:history:separate")
	if players.Data.PlayerHistory != nil || history.Data.Players != nil || len(players.Data.Players) == 0 || len(history.Data.PlayerHistory) == 0 {
		t.Fatalf("players/history projections overlapped: %#v / %#v", players.Data, history.Data)
	}
	access := execute(effect.FleetRuntimeObservationFieldAccessV1, "observe:access:separate")
	snapshot := execute(effect.FleetRuntimeObservationFieldAccessSnapshotV1, "observe:access-snapshot:separate")
	if access.Data.AccessSnapshot != nil || snapshot.Data.Access != nil || access.Data.Access == nil || snapshot.Data.AccessSnapshot == nil {
		t.Fatalf("access projections overlapped: %#v / %#v", access.Data, snapshot.Data)
	}
}

func TestHostScopedRuntimeObservationGameruleProbeIncludesDoInsomnia(t *testing.T) {
	command := fleetObservationGameRulesCommand()
	if !strings.Contains(command, "RULE:doInsomnia:") || !strings.Contains(command, "gamerule doInsomnia") {
		t.Fatalf("gamerule probe missing doInsomnia: %q", command)
	}
}

func TestHostScopedRuntimeObservationDetailedReadsRejectMalformedAndOversizeEvidence(t *testing.T) {
	scope := observationTestScope(t)
	malformedHistory := observationTestRuntimeForScope(t, scope)
	malformedHistory.outputs[strings.Join([]string{"sh", "-c", fleetObservationPlayerHistoryCommand}, "\x00")] = `[{"uuid":"bad","name":"Steve","expiresOn":"2026-08-26 01:00:00 +0000"}]`
	if _, err := observationTestExecutor(t, scope, malformedHistory).ExecuteIntent(context.Background(), scope, observationTestIntent(t, effect.FleetRuntimeObservationFieldPlayerHistoryV1, "observe:history:malformed")); err == nil {
		t.Fatal("malformed player history succeeded")
	}

	malformedSnapshot := observationTestRuntimeForScope(t, scope)
	malformedSnapshot.outputs[strings.Join([]string{"sh", "-c", fleetObservationOpsCommand}, "\x00")] = `[{"uuid":"abcdefab-cdef-abcd-efab-cdefabcdefab","name":"Alex","level":5,"bypassesPlayerLimit":true}]`
	if _, err := observationTestExecutor(t, scope, malformedSnapshot).ExecuteIntent(context.Background(), scope, observationTestIntent(t, effect.FleetRuntimeObservationFieldAccessSnapshotV1, "observe:access-snapshot:malformed")); err == nil {
		t.Fatal("malformed access snapshot succeeded")
	}

	oversize := observationTestRuntimeForScope(t, scope)
	oversize.outputs[strings.Join([]string{"sh", "-c", fleetObservationPlayerHistoryCommand}, "\x00")] = strings.Repeat("x", fleetObservationMaxBytes+1)
	if _, err := observationTestExecutor(t, scope, oversize).ExecuteIntent(context.Background(), scope, observationTestIntent(t, effect.FleetRuntimeObservationFieldPlayerHistoryV1, "observe:history:oversize")); err == nil {
		t.Fatal("oversize player history succeeded")
	}
}
