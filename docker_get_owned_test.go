package dockerext

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/bananalabs-oss/potassium/orchestrator"
	"github.com/tetratelabs/wazero"
	"github.com/vmihailenco/msgpack/v5"
)

type fakeOwnedDockerProvider struct {
	server *orchestrator.Server
	err    error
	got    []string
}

// fiberCompatibleDockerServer pins the independently owned Fiber wire without
// importing Fiber into this extension package.
type fiberCompatibleDockerServer struct {
	ID          string         `msgpack:"id"`
	Name        string         `msgpack:"name"`
	Status      string         `msgpack:"status"`
	IP          string         `msgpack:"ip"`
	Ports       map[string]int `msgpack:"ports"`
	CPULimit    float64        `msgpack:"cpu_limit,omitempty"`
	MemoryLimit int64          `msgpack:"memory_limit,omitempty"`
}

func (p *fakeOwnedDockerProvider) Get(_ context.Context, name string) (*orchestrator.Server, error) {
	p.got = append(p.got, name)
	return p.server, p.err
}

func TestOwnedDockerRuntimeNameScopesHyphenatedLogicalNameExactly(t *testing.T) {
	scope := mustDockerScope(t, "sessions", "prod-a", "fleet", "primary")
	name, err := ownedDockerRuntimeName(scope, "server-east-2")
	if err != nil {
		t.Fatalf("ownedDockerRuntimeName: %v", err)
	}
	want := "pulp-sessions-prod-a-fleet-primary-server-east-2"
	if name != want {
		t.Fatalf("runtime name = %q, want %q", name, want)
	}
	for _, logicalName := range []string{"", "../server", "pulp-sessions-prod-a-fleet-primary-server-east-2", "server east"} {
		if _, err := ownedDockerRuntimeName(scope, logicalName); err == nil {
			t.Errorf("logical name %q was accepted", logicalName)
		}
	}
}

func TestResolveOwnedDockerServerUsesExactScopedNameAndDirectServerWire(t *testing.T) {
	scope := mustDockerScope(t, "sessions", "prod-a", "fleet", "primary")
	logicalName := "server-east-2"
	runtimeName, err := ownedDockerRuntimeName(scope, logicalName)
	if err != nil {
		t.Fatalf("ownedDockerRuntimeName: %v", err)
	}
	provider := &fakeOwnedDockerProvider{server: &orchestrator.Server{
		ID: "container-east-2", Name: "/" + runtimeName, Status: orchestrator.StatusRunning,
		IP: "10.0.0.8", Ports: map[string]int{"25565/tcp": 25565},
	}}
	server, code := resolveOwnedDockerServer(context.Background(), provider, scope, logicalName)
	if code != codeOK {
		t.Fatalf("resolve code = %d", code)
	}
	if !reflect.DeepEqual(provider.got, []string{runtimeName}) {
		t.Fatalf("provider lookup = %#v, want exact %#v", provider.got, []string{runtimeName})
	}
	if server == nil || server.ID != "container-east-2" {
		t.Fatalf("server = %#v", server)
	}
	// The ABI uses the existing lower-case guest Docker Server MessagePack
	// shape, not orchestrator.Server's provider field names.
	wire, err := msgpack.Marshal(serverToResponse(server))
	if err != nil {
		t.Fatalf("marshal guest server: %v", err)
	}
	var decoded fiberCompatibleDockerServer
	if err := msgpack.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("unmarshal Fiber-compatible server: %v", err)
	}
	if decoded.ID != server.ID || decoded.Name != server.Name || decoded.Status != string(server.Status) || decoded.ID == "" || decoded.Name == "" {
		t.Fatalf("Fiber-compatible server wire = %#v, want nonempty %#v", decoded, server)
	}
}

func TestResolveOwnedDockerServerRejectsForeignAndMissingWithoutCrossScopeLeak(t *testing.T) {
	sessions := mustDockerScope(t, "sessions", "prod-a", "fleet", "primary")
	evolution := mustDockerScope(t, "evolution", "prod-a", "fleet", "primary")
	logicalName := "server-east-2"
	foreignName, err := ownedDockerRuntimeName(evolution, logicalName)
	if err != nil {
		t.Fatalf("foreign runtime name: %v", err)
	}
	foreign := &fakeOwnedDockerProvider{server: &orchestrator.Server{ID: "foreign", Name: "/" + foreignName}}
	if server, code := resolveOwnedDockerServer(context.Background(), foreign, sessions, logicalName); code != codeNotFound || server != nil {
		t.Fatalf("foreign server/code = %#v / %d, want nil / %d", server, code, codeNotFound)
	}
	missing := &fakeOwnedDockerProvider{err: errors.New("No such container: missing")}
	if server, code := resolveOwnedDockerServer(context.Background(), missing, sessions, logicalName); code != codeNotFound || server != nil {
		t.Fatalf("missing server/code = %#v / %d, want nil / %d", server, code, codeNotFound)
	}
	broken := &fakeOwnedDockerProvider{err: errors.New("Docker daemon failed")}
	if server, code := resolveOwnedDockerServer(context.Background(), broken, sessions, logicalName); code != codeDockerError || server != nil {
		t.Fatalf("broken server/code = %#v / %d, want nil / %d", server, code, codeDockerError)
	}
	if server, code := resolveOwnedDockerServer(context.Background(), nil, sessions, logicalName); code != codeProviderUnavail || server != nil {
		t.Fatalf("unavailable server/code = %#v / %d, want nil / %d", server, code, codeProviderUnavail)
	}
}

func TestDockerGetOwnedABIIsBoundAndStubbedWithSpawnCapability(t *testing.T) {
	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)
	defer runtime.Close(ctx)
	cell := fakeScopedCell{name: "fleet", scope: mustDockerScope(t, "sessions", "prod-a", "fleet", "primary")}
	active := runtime.NewHostModuleBuilder("docker_get_owned_active")
	if err := bindActive(active, cell); err != nil {
		t.Fatalf("bind active: %v", err)
	}
	activeModule, err := active.Instantiate(ctx)
	if err != nil {
		t.Fatalf("instantiate active: %v", err)
	}
	if _, ok := activeModule.ExportedFunctionDefinitions()["docker_get_owned"]; !ok {
		t.Fatal("active spawn.docker capability did not export docker_get_owned")
	}
	stub := runtime.NewHostModuleBuilder("docker_get_owned_stub")
	if err := bindStub(stub, cell); err != nil {
		t.Fatalf("bind stub: %v", err)
	}
	stubModule, err := stub.Instantiate(ctx)
	if err != nil {
		t.Fatalf("instantiate stub: %v", err)
	}
	if _, ok := stubModule.ExportedFunctionDefinitions()["docker_get_owned"]; !ok {
		t.Fatal("stub spawn.docker capability did not export docker_get_owned")
	}
	if got := dockerGetOwnedStub(ctx, nil, 0, 0, 0, 0); got != codeCapabilityStubbed {
		t.Fatalf("stub code = %d, want %d", got, codeCapabilityStubbed)
	}
}

func TestDockerGetOwnedStateIsRecreatedAfterScopedRestart(t *testing.T) {
	resetDockerScopesForTest(t)
	sessions := mustDockerScope(t, "sessions", "prod-a", "fleet", "primary")
	evolution := mustDockerScope(t, "evolution", "prod-a", "fleet", "primary")
	first, err := dockerStateForScope(sessions)
	if err != nil {
		t.Fatalf("sessions state: %v", err)
	}
	other, err := dockerStateForScope(evolution)
	if err != nil {
		t.Fatalf("Evolution state: %v", err)
	}
	if err := teardownDockerScope(context.Background(), sessions); err != nil {
		t.Fatalf("sessions teardown: %v", err)
	}
	restarted, err := dockerStateForScope(sessions)
	if err != nil {
		t.Fatalf("restarted sessions state: %v", err)
	}
	stillOther, err := dockerStateForScope(evolution)
	if err != nil {
		t.Fatalf("Evolution state after sessions restart: %v", err)
	}
	if restarted == first {
		t.Fatal("sessions restart reused stale Docker state")
	}
	if stillOther != other {
		t.Fatal("sessions restart disturbed Evolution Docker state")
	}
}
