package dockerext

import (
	"testing"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

func TestRegisteredCapabilitiesHaveExactProviderIdentity(t *testing.T) {
	const provider = "github.com/BananaLabs-OSS/Pulp-ext-docker"
	want := map[string]bool{
		"spawn.docker":        false,
		FleetEffectCapability: false,
	}
	for _, capability := range ext.All() {
		if _, tracked := want[capability.Name]; !tracked {
			continue
		}
		if capability.Provider != provider {
			t.Fatalf("%s provider = %q, want exact module identity %q", capability.Name, capability.Provider, provider)
		}
		want[capability.Name] = true
	}
	for name, found := range want {
		if !found {
			t.Fatalf("%s capability was not registered", name)
		}
	}
}
