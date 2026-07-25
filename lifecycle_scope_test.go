package dockerext

import (
	"context"
	"sync"
	"testing"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

func TestDockerCapabilityScopesAreIsolatedAcrossApplications(t *testing.T) {
	resetDockerScopesForTest(t)
	appA := mustDockerScope(t, "evolution", "a", "host", "primary")
	appB := mustDockerScope(t, "sessions", "b", "host", "primary")
	cellA := mustDockerScope(t, "evolution", "a", "fleet", "primary")
	cellB := mustDockerScope(t, "sessions", "b", "fleet", "primary")

	if err := setupDockerScope(ext.SetupEnv{Scope: appA}); err != nil {
		t.Fatalf("setup app A: %v", err)
	}
	if err := setupDockerScope(ext.SetupEnv{Scope: appB}); err != nil {
		t.Fatalf("setup app B: %v", err)
	}
	stateA, err := dockerStateForScope(cellA)
	if err != nil {
		t.Fatalf("state A: %v", err)
	}
	stateB, err := dockerStateForScope(cellB)
	if err != nil {
		t.Fatalf("state B: %v", err)
	}
	if stateA == stateB {
		t.Fatal("two applications unexpectedly share Docker client/build/event state")
	}
	stateB.buildState.LastError = "sessions-only"

	// A cell restart only releases its routing lookup; application state remains
	// live for sibling cells until the application's TeardownScope runs.
	if err := teardownDockerCell(context.Background(), cellA.RoutingID()); err != nil {
		t.Fatalf("cell A teardown: %v", err)
	}
	if got := dockerScopeStateCount(); got != 2 {
		t.Fatalf("states after cell teardown = %d, want 2", got)
	}
	if err := teardownDockerScope(context.Background(), appA); err != nil {
		t.Fatalf("app A teardown: %v", err)
	}
	if got := dockerScopeStateCount(); got != 1 {
		t.Fatalf("states after app A teardown = %d, want 1", got)
	}
	if stateB.buildState.LastError != "sessions-only" {
		t.Fatalf("app B state changed during app A teardown: %#v", stateB.buildState)
	}
	if got := scopePrefix(cellA); got == scopePrefix(cellB) {
		t.Fatalf("scoped container prefixes collide: %q", got)
	}
}

func TestDockerCapabilityRepeatedTwoAppLifecycleIsRaceSafe(t *testing.T) {
	resetDockerScopesForTest(t)
	apps := []ext.Scope{
		mustDockerScope(t, "evolution", "a", "host", "primary"),
		mustDockerScope(t, "sessions", "b", "host", "primary"),
	}
	var wg sync.WaitGroup
	for _, app := range apps {
		app := app
		cell := mustDockerScope(t, app.ApplicationID(), app.ApplicationInstanceID(), "fleet", "primary")
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := setupDockerScope(ext.SetupEnv{Scope: app}); err != nil {
					t.Errorf("setup %s: %v", app.RoutingID(), err)
					return
				}
				if _, err := dockerStateForScope(cell); err != nil {
					t.Errorf("cell state %s: %v", cell.RoutingID(), err)
				}
			}()
		}
	}
	wg.Wait()
	if got := dockerScopeStateCount(); got != 2 {
		t.Fatalf("state count = %d, want 2", got)
	}
	for _, app := range apps {
		if err := teardownDockerScope(context.Background(), app); err != nil {
			t.Fatalf("teardown %s: %v", app.RoutingID(), err)
		}
	}
	if got := dockerScopeStateCount(); got != 0 {
		t.Fatalf("state count after teardown = %d, want 0", got)
	}
}

func mustDockerScope(t *testing.T, app, instance, cell, cellInstance string) ext.Scope {
	t.Helper()
	scope, err := ext.NewScope(app, instance, cell, cellInstance)
	if err != nil {
		t.Fatalf("NewScope: %v", err)
	}
	return scope
}

func dockerScopeStateCount() int {
	dockerScopes.Lock()
	defer dockerScopes.Unlock()
	return len(dockerScopes.states)
}

func resetDockerScopesForTest(t *testing.T) {
	t.Helper()
	dockerScopes.Lock()
	states := dockerScopes.states
	dockerScopes.states = make(map[ext.ResourceKey]*dockerScopeState)
	dockerScopes.routes = make(map[string]ext.ResourceKey)
	dockerScopes.Unlock()
	for _, state := range states {
		if state.eventsCancel != nil {
			state.eventsCancel()
		}
	}
	t.Cleanup(func() { resetDockerScopesForTestCleanup() })
}

func resetDockerScopesForTestCleanup() {
	dockerScopes.Lock()
	states := dockerScopes.states
	dockerScopes.states = make(map[ext.ResourceKey]*dockerScopeState)
	dockerScopes.routes = make(map[string]ext.ResourceKey)
	dockerScopes.Unlock()
	for _, state := range states {
		if state.eventsCancel != nil {
			state.eventsCancel()
		}
	}
}
