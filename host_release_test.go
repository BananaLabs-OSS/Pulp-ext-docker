package dockerext

import (
	"context"
	"testing"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

func TestReleaseHostScopeRebuildsProviderForNewDockerHost(t *testing.T) {
	resetDockerScopesForTest(t)
	ctx := context.Background()
	target := mustScope(t, "sessions-release", "prod-a", "host", "primary")
	targetFleet := mustScope(t, "sessions-release", "prod-a", "fleet", "primary")
	other := mustScope(t, "evolution-release", "prod-a", "host", "primary")
	otherFleet := mustScope(t, "evolution-release", "prod-a", "fleet", "primary")
	t.Cleanup(func() {
		_ = ReleaseHostScope(ctx, target)
		_ = ReleaseHostScope(ctx, other)
	})

	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:23751")
	firstState, err := dockerStateForScope(target)
	if err != nil {
		t.Fatalf("first target state: %v", err)
	}
	firstProvider, err := firstState.ensureProvider()
	if err != nil {
		t.Fatalf("first provider: %v", err)
	}
	if firstState.providerEndpoint != "tcp://127.0.0.1:23751" {
		t.Fatalf("first provider endpoint = %q", firstState.providerEndpoint)
	}
	firstExecutor, err := hostFleetEffectExecutors.ForScope(targetFleet)
	if err != nil {
		t.Fatalf("first target executor: %v", err)
	}

	otherState, err := dockerStateForScope(other)
	if err != nil {
		t.Fatalf("other state: %v", err)
	}
	otherExecutor, err := hostFleetEffectExecutors.ForScope(otherFleet)
	if err != nil {
		t.Fatalf("other executor: %v", err)
	}
	beforeInvalid := hostFleetEffectExecutors.Count()
	if err := ReleaseHostScope(ctx, ext.Scope{}); err == nil {
		t.Fatal("scope-less release succeeded")
	}
	if hostFleetEffectExecutors.Count() != beforeInvalid {
		t.Fatal("scope-less release changed executor state")
	}
	if got, err := dockerStateForScope(other); err != nil || got != otherState {
		t.Fatalf("scope-less release changed other Docker state: %#v, %v", got, err)
	}

	if err := ReleaseHostScope(ctx, target); err != nil {
		t.Fatalf("release first host: %v", err)
	}
	firstState.mu.Lock()
	retainedProvider := firstState.provider
	retainedEndpoint := firstState.providerEndpoint
	firstState.mu.Unlock()
	if retainedProvider != nil || retainedEndpoint != "" {
		t.Fatalf("released state retained provider %p or endpoint %q", retainedProvider, retainedEndpoint)
	}
	if got, err := dockerStateForScope(other); err != nil || got != otherState {
		t.Fatalf("target release changed other Docker state: %#v, %v", got, err)
	}
	if got, err := hostFleetEffectExecutors.ForScope(otherFleet); err != nil || got != otherExecutor {
		t.Fatalf("target release changed other executor: %#v, %v", got, err)
	}

	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:23752")
	secondState, err := dockerStateForScope(target)
	if err != nil {
		t.Fatalf("second target state: %v", err)
	}
	secondProvider, err := secondState.ensureProvider()
	if err != nil {
		t.Fatalf("second provider: %v", err)
	}
	if secondState == firstState || secondProvider == firstProvider {
		t.Fatal("second host reused first host Docker state or provider")
	}
	if secondState.providerEndpoint != "tcp://127.0.0.1:23752" {
		t.Fatalf("second provider endpoint = %q", secondState.providerEndpoint)
	}
	secondExecutor, err := hostFleetEffectExecutors.ForScope(targetFleet)
	if err != nil {
		t.Fatalf("second target executor: %v", err)
	}
	if secondExecutor == firstExecutor {
		t.Fatal("second host reused first host executor/replay cache")
	}

	if err := ReleaseHostScope(ctx, target); err != nil {
		t.Fatalf("release second host: %v", err)
	}
	if err := ReleaseHostScope(ctx, target); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if got, err := dockerStateForScope(other); err != nil || got != otherState {
		t.Fatalf("replayed target release changed other state: %#v, %v", got, err)
	}
}
