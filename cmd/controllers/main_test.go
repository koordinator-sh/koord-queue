package main

import (
	"testing"

	"github.com/koordinator-sh/koord-queue/pkg/features"
	"github.com/stretchr/testify/require"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

func TestApplyFeatureGates(t *testing.T) {
	require.NoError(t, applyFeatureGates("QueueUnitActive=true"))
	t.Cleanup(func() {
		require.NoError(t, applyFeatureGates("QueueUnitActive=false"))
	})

	require.True(t, utilfeature.DefaultFeatureGate.Enabled(features.QueueUnitActive))
}

func TestApplyFeatureGatesRejectsMissingDependencies(t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, applyFeatureGates("MaximumExecutionTime=false,QueueUnitActive=false,QueueUnitConditions=true"))
	})

	require.Error(t, applyFeatureGates("MaximumExecutionTime=true,QueueUnitActive=false"))
}
