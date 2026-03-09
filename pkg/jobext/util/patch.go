package util

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

func CreateMergePatch(old, new interface{}) ([]byte, error) {
	oldBytes, err := json.Marshal(old)
	if err != nil {
		return nil, err
	}
	newBytes, err := json.Marshal(new)
	if err != nil {
		return nil, err
	}

	return strategicpatch.CreateTwoWayMergePatch(oldBytes, newBytes, new)
}
