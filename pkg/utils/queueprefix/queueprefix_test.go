package queueprefix

import (
	"fmt"
	"reflect"
	"testing"
)

// The tests in this file are deliberately flavor-agnostic: every expectation is derived
// from acceptedPrefixes rather than hardcoding a prefix. The same file therefore passes
// both upstream (single koord-queue/ prefix) and in the internal ACK build (kube-queue/
// canonical plus koord-queue/ alias), so switching flavors never requires editing tests.

const testName = "max-depth"

func canonicalPrefix() string { return acceptedPrefixes[0] }

func canonicalKey() string { return canonicalPrefix() + testName }

// acceptedSpellings builds every accepted spelling of the test key, canonical first.
func acceptedSpellings() []string {
	out := make([]string, 0, len(acceptedPrefixes))
	for _, p := range acceptedPrefixes {
		out = append(out, p+testName)
	}
	return out
}

// unacceptedPrefixes returns the well-known prefixes this build does not accept.
func unacceptedPrefixes() []string {
	var out []string
	for _, p := range knownPrefixes {
		accepted := false
		for _, a := range acceptedPrefixes {
			if a == p {
				accepted = true
				break
			}
		}
		if !accepted {
			out = append(out, p)
		}
	}
	return out
}

func multiPrefix() bool { return len(acceptedPrefixes) > 1 }

// TestAcceptedPrefixesContract pins the portability contract: the accepted set is the
// only flavor-specific state, it is non-empty, duplicate-free, canonical first, and
// drawn from the known prefixes.
func TestAcceptedPrefixesContract(t *testing.T) {
	if len(acceptedPrefixes) == 0 {
		t.Fatal("acceptedPrefixes must not be empty")
	}
	seen := map[string]bool{}
	for _, p := range acceptedPrefixes {
		known := false
		for _, k := range knownPrefixes {
			if k == p {
				known = true
				break
			}
		}
		if !known {
			t.Errorf("accepted prefix %q is not a known prefix", p)
		}
		if seen[p] {
			t.Errorf("accepted prefix %q is duplicated", p)
		}
		seen[p] = true
	}
	if Canonical(acceptedPrefixes[len(acceptedPrefixes)-1]+testName) != canonicalKey() {
		t.Error("Canonical must normalize every accepted spelling to the canonical one")
	}
}

func TestCanonical(t *testing.T) {
	for _, p := range knownPrefixes {
		if got, want := Canonical(p+testName), canonicalKey(); got != want {
			t.Errorf("Canonical(%q) = %q, want %q", p+testName, got, want)
		}
	}
	for _, in := range []string{"unrelated/key", "", testName} {
		if got := Canonical(in); got != in {
			t.Errorf("Canonical(%q) = %q, want it unchanged", in, got)
		}
	}
}

func TestAlias(t *testing.T) {
	// Alias is a pure string swap between the two well-known prefixes.
	if got, want := Alias(KoordQueue+testName), KubeQueue+testName; got != want {
		t.Errorf("Alias(koord) = %q, want %q", got, want)
	}
	if got, want := Alias(KubeQueue+testName), KoordQueue+testName; got != want {
		t.Errorf("Alias(kube) = %q, want %q", got, want)
	}
	if got := Alias("unrelated/key"); got != "unrelated/key" {
		t.Errorf("Alias(unprefixed) = %q, want it unchanged", got)
	}
}

func TestKeysReturnsAcceptedSpellingsCanonicalFirst(t *testing.T) {
	want := acceptedSpellings()
	for _, p := range knownPrefixes {
		if got := Keys(p + testName); !reflect.DeepEqual(got, want) {
			t.Errorf("Keys(%q) = %v, want %v", p+testName, got, want)
		}
	}
	if got := Keys("unrelated/key"); !reflect.DeepEqual(got, []string{"unrelated/key"}) {
		t.Errorf("Keys(unprefixed) = %v, want no alias", got)
	}
}

func TestHasPrefix(t *testing.T) {
	for _, p := range acceptedPrefixes {
		if !HasPrefix(p + testName) {
			t.Errorf("HasPrefix(%q) = false, want true", p+testName)
		}
	}
	for _, p := range unacceptedPrefixes() {
		if HasPrefix(p + testName) {
			t.Errorf("HasPrefix(%q) = true, want false (prefix not accepted by this build)", p+testName)
		}
	}
	for _, in := range []string{"other/x", testName, ""} {
		if HasPrefix(in) {
			t.Errorf("HasPrefix(%q) = true, want false", in)
		}
	}
}

func TestGetAcrossAcceptedSpellings(t *testing.T) {
	// Each accepted spelling alone is found no matter which spelling the caller asks for.
	for _, set := range acceptedSpellings() {
		mp := map[string]string{set: "7"}
		for _, ask := range acceptedSpellings() {
			if v, ok := Get(mp, ask); v != "7" || !ok {
				t.Errorf("Get(set=%q, ask=%q) = %q,%v; want 7,true", set, ask, v, ok)
			}
		}
	}

	// A key written only with an unaccepted prefix is invisible.
	for _, p := range unacceptedPrefixes() {
		mp := map[string]string{p + testName: "5"}
		if v, ok := Get(mp, canonicalKey()); v != "" || ok {
			t.Errorf("Get(unaccepted %q) = %q,%v; want \"\",false", p+testName, v, ok)
		}
	}

	// Present-but-empty reports presence; GetNonEmpty is the variant that skips it.
	empty := map[string]string{canonicalKey(): ""}
	if _, ok := Get(empty, canonicalKey()); !ok {
		t.Error("Get should report presence for an empty value")
	}
	if v, ok := GetNonEmpty(empty, canonicalKey()); v != "" || ok {
		t.Errorf("GetNonEmpty(blank) = %q,%v; want \"\",false", v, ok)
	}

	// Absent key and nil map.
	if v, ok := Get(map[string]string{"other": "x"}, canonicalKey()); v != "" || ok {
		t.Errorf("Get(absent) = %q,%v; want \"\",false", v, ok)
	}
	if v, ok := Get(nil, canonicalKey()); v != "" || ok {
		t.Errorf("Get(nil) = %q,%v; want \"\",false", v, ok)
	}
	if Has(nil, canonicalKey()) {
		t.Error("Has(nil) should be false")
	}
}

func TestCanonicalWinsWhenEverySpellingIsSet(t *testing.T) {
	mp := map[string]string{}
	for i, k := range acceptedSpellings() {
		mp[k] = fmt.Sprint(i)
	}
	// acceptedSpellings is canonical first, so index 0 must win.
	if v, ok := Get(mp, canonicalKey()); v != "0" || !ok {
		t.Errorf("Get = %q,%v; want \"0\",true (canonical precedence)", v, ok)
	}
	if got := Value(mp, Alias(canonicalKey())); got != "0" {
		t.Errorf("Value via alias = %q, want \"0\"", got)
	}
}

func TestGetNonEmptyFallsBackToAlias(t *testing.T) {
	if !multiPrefix() {
		t.Skip("blank-value fallback needs more than one accepted prefix")
	}
	alias := Alias(canonicalKey())
	mp := map[string]string{canonicalKey(): "", alias: "Intelligent"}
	if got := ValueNonEmpty(mp, canonicalKey()); got != "Intelligent" {
		t.Errorf("ValueNonEmpty = %q, want Intelligent (blank canonical defers to alias)", got)
	}
	allBlank := map[string]string{canonicalKey(): "", alias: ""}
	if v, ok := GetNonEmpty(allBlank, canonicalKey()); v != "" || ok {
		t.Errorf("GetNonEmpty(all blank) = %q,%v; want \"\",false", v, ok)
	}
}

func TestValueAndHas(t *testing.T) {
	aliasOnly := map[string]string{}
	if multiPrefix() {
		aliasOnly[Alias(canonicalKey())] = "true"
	} else {
		aliasOnly[canonicalKey()] = "true"
	}
	if got := Value(aliasOnly, canonicalKey()); got != "true" {
		t.Errorf("Value = %q, want true", got)
	}
	if !Has(aliasOnly, canonicalKey()) {
		t.Error("Has should find an accepted spelling")
	}
	if got := Value(aliasOnly, canonicalPrefix()+"absent"); got != "" {
		t.Errorf("Value(absent) = %q, want empty", got)
	}
}

func TestDeleteRemovesEveryAcceptedSpelling(t *testing.T) {
	mp := map[string]string{"keep": "c"}
	for i, k := range acceptedSpellings() {
		mp[k] = fmt.Sprint(i)
	}
	Delete(mp, Alias(canonicalKey()))
	if !reflect.DeepEqual(mp, map[string]string{"keep": "c"}) {
		t.Errorf("Delete left %v, want only keep", mp)
	}
	Delete(nil, canonicalKey()) // must not panic
}

func TestSetWritesCanonicalOnly(t *testing.T) {
	mp := map[string]string{}
	Set(mp, Alias(canonicalKey()), "q1")
	if mp[canonicalKey()] != "q1" {
		t.Errorf("Set wrote %v, want the canonical spelling", mp)
	}
	if len(mp) != 1 {
		t.Errorf("Set must write exactly one key, got %v", mp)
	}
	Set(nil, canonicalKey(), "v") // must not panic
}

func TestSyncWritesCanonicalAndRefreshesPresentAliases(t *testing.T) {
	// No alias present: only the canonical key is written.
	mp := map[string]string{}
	Sync(mp, canonicalKey(), "root/a")
	if mp[canonicalKey()] != "root/a" || len(mp) != 1 {
		t.Errorf("Sync wrote %v, want only the canonical key", mp)
	}

	if !multiPrefix() {
		// With one accepted prefix Sync must not resurrect an unaccepted spelling.
		mp = map[string]string{Alias(canonicalKey()): "stale"}
		Sync(mp, canonicalKey(), "fresh")
		if mp[Alias(canonicalKey())] != "stale" {
			t.Errorf("Sync touched an unaccepted spelling: %v", mp)
		}
		return
	}

	// Alias present: every accepted spelling ends up consistent so they never diverge.
	alias := Alias(canonicalKey())
	mp = map[string]string{canonicalKey(): "old", alias: "old"}
	Sync(mp, alias, "root/new")
	if mp[canonicalKey()] != "root/new" || mp[alias] != "root/new" {
		t.Errorf("Sync left divergent values: %v", mp)
	}
	Sync(nil, canonicalKey(), "v") // must not panic
}

func TestSkipAlias(t *testing.T) {
	mp := map[string]string{}
	for i, k := range acceptedSpellings() {
		mp[k] = fmt.Sprint(i)
	}
	if SkipAlias(mp, canonicalKey()) {
		t.Error("SkipAlias must never skip the canonical spelling")
	}
	for _, k := range acceptedSpellings()[1:] {
		if !SkipAlias(mp, k) {
			t.Errorf("SkipAlias(%q) = false, want true when the canonical key is also present", k)
		}
	}
	// Without the canonical key present, an alias must not be skipped.
	aliasOnly := map[string]string{}
	if multiPrefix() {
		aliasOnly[Alias(canonicalKey())] = "9"
		if SkipAlias(aliasOnly, Alias(canonicalKey())) {
			t.Error("SkipAlias must not skip the alias when it is the only spelling")
		}
	}
	for _, p := range unacceptedPrefixes() {
		if SkipAlias(mp, p+testName) {
			t.Errorf("SkipAlias(%q) = true, want false for an unaccepted prefix", p+testName)
		}
	}
	if SkipAlias(nil, canonicalKey()) {
		t.Error("SkipAlias(nil) should be false")
	}
}

func TestIndexValuesEmitsOneEntryPerAcceptedSpelling(t *testing.T) {
	// The Queue indexer used to return exactly []string{annotations[key]}, including the
	// empty string when the annotation was absent. IndexValues keeps that shape per
	// accepted spelling so cache indexing cannot silently change.
	mp := map[string]string{}
	for i, k := range acceptedSpellings() {
		mp[k] = fmt.Sprintf("root/%d", i)
	}
	want := make([]string, 0, len(acceptedPrefixes))
	for i := range acceptedPrefixes {
		want = append(want, fmt.Sprintf("root/%d", i))
	}
	if got := IndexValues(mp, Alias(canonicalKey())); !reflect.DeepEqual(got, want) {
		t.Errorf("IndexValues = %v, want %v", got, want)
	}
	blanks := make([]string, len(acceptedPrefixes))
	if got := IndexValues(map[string]string{}, canonicalKey()); !reflect.DeepEqual(got, blanks) {
		t.Errorf("IndexValues(absent) = %v, want %v", got, blanks)
	}
	if got := IndexValues(nil, canonicalKey()); !reflect.DeepEqual(got, blanks) {
		t.Errorf("IndexValues(nil) = %v, want %v", got, blanks)
	}
}
