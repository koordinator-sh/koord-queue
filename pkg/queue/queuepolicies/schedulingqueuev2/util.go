package schedulingqueuev2

import "sync"

type syncInt struct {
	sync.Map
}

func (s *syncInt) Store(key string, value int) {
	s.Map.Store(key, value)
}

func (s *syncInt) Load(key string) (int, bool) {
	v, ok := s.Map.Load(key)
	if !ok {
		return 0, false
	}
	vi, ok := v.(int)
	if !ok {
		return 0, false
	}
	return vi, ok
}

func (s *syncInt) LoadOrStore(key string, value int) (int, bool) {
	v, ok := s.Map.LoadOrStore(key, value)
	if !ok {
		return value, false
	}
	vi, ok := v.(int)
	if !ok {
		return 0, true
	}
	return vi, true
}

func (s *syncInt) Delete(key string) {
	s.Map.Delete(key)
}
