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
		"/",
		"/var/run/docker.sock",
		"/run/docker.sock",
		"/etc",
		"/etc/passwd",
		"/proc",
		"/sys",
		"/root",
		"/home/user",
		"/var/lib/docker",
	}
	for _, src := range denied {
		if err := validateMountSource(src, roots); err == nil {
			t.Errorf("validateMountSource(%q) = nil; want rejection", src)
		}
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
