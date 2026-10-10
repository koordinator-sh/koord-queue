// Package queueprefix is the single place that knows how queue annotations and
// labels are keyed.
//
// Two prefixes are in circulation for this project: "koord-queue/" is used by the
// upstream koord-queue flavor and "kube-queue/" by the ACK (ack-kube-queue)
// flavor. Every read of a queue key should go through this package so that the set
// of accepted spellings is decided in exactly one place instead of being scattered
// across the queue, plugin and job-extension code.
//
// Which spellings are accepted is controlled by acceptedPrefixes below, whose first
// entry is the canonical one:
//
//   - upstream koord-queue keeps a single entry, KoordQueue
//   - the internal ACK build lists KubeQueue first and accepts KoordQueue as an alias
//
// Adding or reordering entries is the only change needed to move between flavors;
// no call site has to be touched. The rules that apply when more than one prefix is
// accepted are documented on the accessors below.
//
// Writes always use the canonical prefix. Keeping a single canonical spelling on the
// write path avoids duplicating state on live objects and keeps diffs, indexers and
// e2e assertions stable, while the read path stays tolerant of whatever prefixes are
// accepted.
package queueprefix

import "strings"

const (
	// KoordQueue is the prefix used by the upstream koord-queue flavor.
	KoordQueue = "koord-queue/"
	// KubeQueue is the prefix used by the ACK (ack-kube-queue) flavor.
	KubeQueue = "kube-queue/"
)

// knownPrefixes lists every prefix this package can recognize, independently of
// which ones the current build accepts.
var knownPrefixes = []string{KoordQueue, KubeQueue}

// acceptedPrefixes lists the prefixes honored on read, canonical first.
var acceptedPrefixes = []string{KoordQueue}

// splitPrefix strips a known prefix from key, reporting whether one was present.
func splitPrefix(key string) (suffix string, ok bool) {
	for _, p := range knownPrefixes {
		if strings.HasPrefix(key, p) {
			return key[len(p):], true
		}
	}
	return key, false
}

// Canonical returns key rewritten to carry the canonical prefix. Keys without a
// recognised prefix are returned unchanged.
func Canonical(key string) string {
	if suffix, ok := splitPrefix(key); ok {
		return acceptedPrefixes[0] + suffix
	}
	return key
}

// Alias returns key rewritten to carry the other well-known prefix. It is a pure
// string transformation and does not consult acceptedPrefixes; use Keys to
// enumerate the spellings this build actually reads.
func Alias(key string) string {
	switch {
	case strings.HasPrefix(key, KoordQueue):
		return KubeQueue + key[len(KoordQueue):]
	case strings.HasPrefix(key, KubeQueue):
		return KoordQueue + key[len(KubeQueue):]
	default:
		return key
	}
}

// Keys returns every spelling of key that this build accepts, canonical first.
// Keys without a recognised prefix have no alias and are returned as-is.
func Keys(key string) []string {
	suffix, ok := splitPrefix(key)
	if !ok {
		return []string{key}
	}
	out := make([]string, 0, len(acceptedPrefixes))
	for _, p := range acceptedPrefixes {
		out = append(out, p+suffix)
	}
	return out
}

// HasPrefix reports whether key carries one of the accepted prefixes.
func HasPrefix(key string) bool {
	for _, p := range acceptedPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Get looks key up in mp across every accepted spelling, canonical first, so the
// canonical value wins when more than one prefix is accepted and both are set.
// The second return value reports whether any spelling was found; note that a
// present-but-empty value reports true.
func Get(mp map[string]string, key string) (string, bool) {
	if mp == nil {
		return "", false
	}
	for _, k := range Keys(key) {
		if v, ok := mp[k]; ok {
			return v, true
		}
	}
	return "", false
}

// GetNonEmpty is Get but skips spellings whose value is the empty string, so an
// explicitly blanked canonical key falls back to an accepted alias. Callers that
// treat an empty value as "unset" should use this instead of Get.
func GetNonEmpty(mp map[string]string, key string) (string, bool) {
	if mp == nil {
		return "", false
	}
	for _, k := range Keys(key) {
		if v, ok := mp[k]; ok && v != "" {
			return v, true
		}
	}
	return "", false
}

// Value is Get but returns the zero value when the key is absent.
func Value(mp map[string]string, key string) string {
	v, _ := Get(mp, key)
	return v
}

// ValueNonEmpty is GetNonEmpty but returns the zero value when no accepted
// spelling holds a non-empty value.
func ValueNonEmpty(mp map[string]string, key string) string {
	v, _ := GetNonEmpty(mp, key)
	return v
}

// Has reports whether any accepted spelling of key is present in mp.
func Has(mp map[string]string, key string) bool {
	_, ok := Get(mp, key)
	return ok
}

// Delete removes every accepted spelling of key from mp. It is safe to call with
// a nil map.
func Delete(mp map[string]string, key string) {
	if mp == nil {
		return
	}
	for _, k := range Keys(key) {
		delete(mp, k)
	}
}

// Set stores value under the canonical spelling of key.
func Set(mp map[string]string, key, value string) {
	if mp == nil {
		return
	}
	mp[Canonical(key)] = value
}

// Sync stores value under the canonical spelling of key and, for every other
// accepted spelling already present on the object, refreshes it too so accepted
// spellings never diverge.
func Sync(mp map[string]string, key, value string) {
	if mp == nil {
		return
	}
	keys := Keys(key)
	mp[keys[0]] = value
	for _, k := range keys[1:] {
		if _, ok := mp[k]; ok {
			mp[k] = value
		}
	}
}

// SkipAlias reports whether key is a non-canonical accepted spelling while the
// canonical spelling is also present in mp. Callers that copy a whole map key by
// key must skip such keys, otherwise which value survives depends on Go's
// randomized map iteration order.
func SkipAlias(mp map[string]string, key string) bool {
	if !HasPrefix(key) {
		return false
	}
	canonical := Canonical(key)
	if key == canonical {
		return false
	}
	_, ok := mp[canonical]
	return ok
}

// IndexValues returns one value per accepted spelling of key, canonical first,
// reading the zero value for spellings that are absent. It is intended for cache
// IndexFunc implementations so that an object indexed under one accepted spelling
// is also found when the caller looks it up by another.
func IndexValues(mp map[string]string, key string) []string {
	keys := Keys(key)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, mp[k])
	}
	return out
}
