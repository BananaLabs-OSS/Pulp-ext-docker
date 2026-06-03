package dockerext

import (
	"path/filepath"
	"testing"
)

func TestValidateMountSource_DeniesProtectedPaths(t *testing.T) {
	// Allow a wide root so failures are attributable to the deny-list, not the
	// allow-root check.
	roots := []string{"/"}
	denied := []string{
		"/",                     // host root, exact-match deny (full-host takeover)
		"/var/run/docker.sock",  // docker socket
		"/run/docker.sock",      // docker socket (alt path)
		"/var/run",              // includes the docker socket dir
		"/var/run/secrets",      // nested under a denied prefix
		"/run",                  // includes the docker socket dir (alt)
		"/etc",                  // host config
		"/etc/passwd",           // nested under /etc
		"/proc",                 // kernel
		"/sys",                  // kernel
		"/dev",                  // devices
		"/boot",                 // bootloader/kernel
		"/root",                 // root home
		"/var/lib/docker",       // docker state
		"/var/lib/docker/volumes/x", // nested under docker state
	}
	for _, src := range denied {
		if err := validateMountSource(src, roots); err == nil {
			t.Errorf("validateMountSource(%q) = nil; want rejection", src)
		}
	}
}

// TestValidateMountSource_AllowsRealWorldMounts pins the legit Bananagine
// provisioning sources: removing "/" from the deny prefix list (the HIGH
// regression) must let absolute world mounts through under a configured root.
//
// Paths are built from a temp dir so the assertion is OS-agnostic: on the
// Windows audit host filepath.Abs prepends a drive letter, which would dodge
// the unix-shaped literals — using t.TempDir() makes source and root share the
// same volume on every platform. The shape mirrors the real templates:
// /var/worlds/<id>:/data (bedrock/paper), /var/sessions/worlds/<id> (worlds_dir
// default), /var/lib/gameserver/.../config.json (hytale).
func TestValidateMountSource_AllowsRealWorldMounts(t *testing.T) {
	base := t.TempDir()
	worldsRoot := filepath.Join(base, "var", "worlds")
	sessionsRoot := filepath.Join(base, "var", "sessions", "worlds")
	gameserverRoot := filepath.Join(base, "var", "lib", "gameserver")
	roots := []string{worldsRoot, sessionsRoot, gameserverRoot}

	allowed := []string{
		filepath.Join(worldsRoot, "srv-1"),                                            // bedrock/paper template
		filepath.Join(sessionsRoot, "srv-1"),                                          // worlds_dir default
		filepath.Join(gameserverRoot, "hycraft-server", "server-config", "config.json"), // hytale template
	}
	for _, src := range allowed {
		if err := validateMountSource(src, roots); err != nil {
			t.Errorf("validateMountSource(%q) under real bind root = %v; want allow", src, err)
		}
	}
}

// TestValidateMountSource_VarSiblingsSegmentDistinct verifies the segment-aware
// matching that lets /var/worlds be allowed while /var/run stays denied — a
// naive /var prefix-deny would wrongly catch the worlds mount.
func TestValidateMountSource_VarSiblingsSegmentDistinct(t *testing.T) {
	// /var/run is denied via deniedMountPaths; /var/worlds is not. Allow root "/"
	// so the result is attributable to the deny-list segment logic.
	roots := []string{"/"}
	if err := validateMountSource("/var/run/docker.sock", roots); err == nil {
		t.Error("/var/run/docker.sock must be denied")
	}
	if !pathContains("/var/run", "/var/run/docker.sock") {
		t.Error("/var/run should contain its docker.sock")
	}
	if pathContains("/var/run", "/var/worlds/srv-1") {
		t.Error("/var/run must NOT prefix-match /var/worlds (segment boundary)")
	}
}

func TestValidateMountSource_RequiresAllowedRoot(t *testing.T) {
	dir := t.TempDir()
	roots := []string{dir}

	inside := filepath.Join(dir, "cell-data", "world")
	if err := validateMountSource(inside, roots); err != nil {
		t.Errorf("validateMountSource(%q) under allowed root = %v; want nil", inside, err)
	}

	// A path outside any allowed root must be rejected even though it is not
	// on the deny-list.
	outside := filepath.Join(filepath.Dir(dir), "elsewhere")
	if err := validateMountSource(outside, roots); err == nil {
		t.Errorf("validateMountSource(%q) outside roots = nil; want rejection", outside)
	}
}

func TestValidateMountSource_NoRootsFailsClosed(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "data")
	if err := validateMountSource(target, nil); err == nil {
		t.Errorf("validateMountSource with no allowed roots = nil; want fail-closed rejection")
	}
}

func TestValidateMountSource_SiblingPrefixNotAllowed(t *testing.T) {
	dir := t.TempDir()
	roots := []string{filepath.Join(dir, "templates")}
	// /…/templates-evil must not pass the /…/templates root check.
	sibling := filepath.Join(dir, "templates-evil")
	if err := validateMountSource(sibling, roots); err == nil {
		t.Errorf("sibling-prefix path %q was allowed; want rejection", sibling)
	}
}

func TestValidateVolumes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_BIND_ROOTS", dir)

	good := map[string]string{
		filepath.Join(dir, "a"): "/data/a",
	}
	if err := validateVolumes(good); err != nil {
		t.Errorf("validateVolumes(good) = %v; want nil", err)
	}

	bad := map[string]string{
		filepath.Join(dir, "a"): "/data/a",
		"/var/run/docker.sock":  "/var/run/docker.sock",
	}
	if err := validateVolumes(bad); err == nil {
		t.Error("validateVolumes(bad) = nil; want rejection for docker socket")
	}

	if err := validateVolumes(nil); err != nil {
		t.Errorf("validateVolumes(nil) = %v; want nil", err)
	}
}

func TestPathContains(t *testing.T) {
	cases := []struct {
		base, target string
		want         bool
	}{
		{"/etc", "/etc", true},
		{"/etc", "/etc/passwd", true},
		{"/etc", "/etcfoo", false},
		{"/", "/anything", true},
		{"/data", "/data2", false},
		{"/data", "/data/x", true},
	}
	for _, c := range cases {
		if got := pathContains(c.base, c.target); got != c.want {
			t.Errorf("pathContains(%q,%q) = %v; want %v", c.base, c.target, got, c.want)
		}
	}
}

func TestCellPrefixAndOwnership(t *testing.T) {
	cellID := "evolution"
	prefix := cellPrefix(cellID)
	if prefix != "pulp-evolution-" {
		t.Errorf("cellPrefix = %q; want pulp-evolution-", prefix)
	}
	if !nameOwnedByCell("/pulp-evolution-mc-1", cellID) {
		t.Error("expected /pulp-evolution-mc-1 to be owned by evolution")
	}
	if nameOwnedByCell("/pulp-other-mc-1", cellID) {
		t.Error("did not expect /pulp-other-mc-1 to be owned by evolution")
	}
	// Cross-cell sibling whose name merely contains the prefix later must not
	// match (prefix is anchored at the start).
	if nameOwnedByCell("/x-pulp-evolution-mc", cellID) {
		t.Error("prefix must be anchored at name start")
	}
}

func TestSanitizeCellID(t *testing.T) {
	if got := sanitizeCellID("a/b..c"); got != "a_b..c" {
		t.Errorf("sanitizeCellID = %q; want a_b..c", got)
	}
	if got := sanitizeCellID(""); got != "cell" {
		t.Errorf("sanitizeCellID(empty) = %q; want cell", got)
	}
}

func TestScopingDisabled(t *testing.T) {
	t.Setenv("DOCKER_SCOPE_DISABLE", "1")
	if !scopingDisabled() {
		t.Error("DOCKER_SCOPE_DISABLE=1 should disable scoping")
	}
	t.Setenv("DOCKER_SCOPE_DISABLE", "")
	if scopingDisabled() {
		t.Error("empty DOCKER_SCOPE_DISABLE should keep scoping enabled")
	}
}
