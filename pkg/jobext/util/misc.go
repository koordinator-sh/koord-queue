package util

func MapCopy[K comparable, V any](m map[K]V) (c map[K]V) {
	c = make(map[K]V, len(m))
	for k, v := range m {
		c[k] = v
	}
	return
}

func SliceCopy[V any](s []V) (c []V) {
	c = make([]V, len(s))
	copy(c, s)
	return
}
