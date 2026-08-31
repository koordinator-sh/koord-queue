package features

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

const (
	// owner: @yueming
	// alpha: v0.6.5
	// beta: v0.9.1
	// Enables elastic quota.
	ElasticQuota featuregate.Feature = "ElasticQuota"
	// owner: @yueming
	// beta: v0.10.0
	//
	// Enables ElasticQuotaTreeDecoupledQueue when using ElasticQuotaTree.
	ElasticQuotaTreeDecoupledQueue featuregate.Feature = "ElasticQuotaTreeDecoupledQueue"
	// owner: @yueming
	// beta: v0.10.0
	//
	// Enables ElasticQuotaTreeBuildQueueForQuota when using ElasticQuotaTree.
	ElasticQuotaTreeBuildQueueForQuota featuregate.Feature = "ElasticQuotaTreeBuildQueueForQuota"
	// owner: @yueming
	// alpha: v0.10.0
	//
	// Enables Available Quota Checking when using ElasticQuotaTree.
	ElasticQuotaTreeCheckAvailableQuota featuregate.Feature = "ElasticQuotaTreeCheckAvailableQuota"
	// owner: @yueming
	// beta: v1.3.0
	//
	// Enables mirroring the QueueUnit phase into status.conditions using the standard
	// metav1.Condition representation. Phase stays authoritative.
	QueueUnitConditions featuregate.Feature = "QueueUnitConditions"
	// owner: @yueming
	// alpha: v1.3.0
	//
	// Enables QueueUnit spec.active, which stops a queue unit from being admitted and
	// evicts it if already admitted.
	QueueUnitActive featuregate.Feature = "QueueUnitActive"
	// owner: @yueming
	// alpha: v1.3.0
	//
	// Enables structured exponential backoff re-queueing tracked in status.requeueState.
	QueueUnitRequeueState featuregate.Feature = "QueueUnitRequeueState"
	// owner: @yueming
	// alpha: v1.3.0
	//
	// Enables deactivating a queue unit once it exceeds spec.maximumExecutionTimeSeconds.
	MaximumExecutionTime featuregate.Feature = "MaximumExecutionTime"
)

func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.Add(defaultFeatureGates))
}

// defaultFeatureGates consists of all known Kueue-specific feature keys.
// To add a new feature, define a key for it above and add it here. The features will be
// available throughout Kueue binaries.
//
// Entries are separated from each other with blank lines to avoid sweeping gofmt changes
// when adding or removing one entry.
var defaultFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	ElasticQuota:                        {Default: true, PreRelease: featuregate.Beta},
	ElasticQuotaTreeDecoupledQueue:      {Default: true, PreRelease: featuregate.Beta},
	ElasticQuotaTreeBuildQueueForQuota:  {Default: true, PreRelease: featuregate.Beta},
	ElasticQuotaTreeCheckAvailableQuota: {Default: false, PreRelease: featuregate.Alpha},

	QueueUnitConditions: {Default: true, PreRelease: featuregate.Beta},

	QueueUnitActive: {Default: false, PreRelease: featuregate.Alpha},

	QueueUnitRequeueState: {Default: false, PreRelease: featuregate.Alpha},

	MaximumExecutionTime: {Default: false, PreRelease: featuregate.Alpha},
}

// Helper for `utilfeature.DefaultFeatureGate.Enabled()`
func Enabled(f featuregate.Feature) bool {
	return utilfeature.DefaultFeatureGate.Enabled(f)
}
