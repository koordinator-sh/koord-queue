# Koord-queue Release Notes

## v1.9.0 (Unreleased)

### Highlights

- Align the QueueUnit API with Kueue Workload concepts by adding conditions, activation, persistent requeue state, maximum execution time, reclaimable pods, and accumulated execution time fields.
- Improve quota utilization by releasing admission for externally deleted Pods, permanently failed Indexed Job indexes, over-admission, and replicas that do not start before `partialRunningTimeout`.
- Strengthen ElasticQuotaV2 integration with `koord-queue/*` metadata propagation, queue-policy compatibility, preemption events, and safer quota accounting.
- Make the v1.9 release reproducible with complete unit and integration coverage, generated-CRD drift checks, Helm validation, and gated multi-architecture image publication.

### QueueUnit API and lifecycle

- `status.conditions` mirrors the authoritative QueueUnit phase using standard Kubernetes conditions. This Beta feature is enabled by default through `QueueUnitConditions`.
- `spec.active` can stop admission and evict an admitted workload. Jobs can set it through the `scheduling.x-k8s.io/active` annotation. This Alpha feature requires `QueueUnitActive`.
- `status.requeueState` persists exponential backoff state across controller restarts. This Alpha feature requires `QueueUnitRequeueState` and `QueueUnitConditions`.
- `spec.maximumExecutionTimeSeconds` limits execution time measured from PodsReady and deactivates the QueueUnit when the limit expires. This Alpha feature requires `MaximumExecutionTime`, `QueueUnitActive`, and `QueueUnitConditions`.
- QueueUnit CRDs include `reclaimablePods` and `accumulatedPastExecutionTimeSeconds`. `reclaimablePods` is API groundwork and is not yet consumed by production scheduling logic.

### Job extensions and resource accounting

- Add configurable `runningTimeout`, `backoffTimeout`, and `partialRunningTimeout` settings for Kubernetes Job, TFJob, and PyTorchJob extensions.
- Reduce admitted replicas to the observed running count after `partialRunningTimeout`, releasing quota held by replicas that never start.
- Release quota when admitted Pods are deleted outside the normal reclaim flow.
- Release Indexed Job admission when indexes fail permanently.
- Honor `scheduling.x-k8s.io/priority` when a QueueUnit is first created, not only on later updates.
- Harden Pod reclaim for TFJob and PyTorchJob and make partial-running timeout state safe across concurrent reconciles and QueueUnit lifecycle transitions.

### Queueing, quota, and observability

- Fix MultiSchedulingQueue deadlocks and stale QueueUnit-to-Queue mappings.
- Improve ElasticQuotaV2 queue metadata synchronization and accept both `koord-queue/queue-policy` and the legacy `kube-queue/queue-policy` annotation.
- Emit Kubernetes events when workloads are preempted or reclaimed.
- Fix over-admission, preemption recovery, nil-map handling, and scheduled-Pod accounting paths.

### Deployment and release

- Publish immutable `v1.9.0` defaults for both images:
  - `ghcr.io/koordinator-sh/koord-queue:v1.9.0`
  - `ghcr.io/koordinator-sh/koord-queue-controllers:v1.9.0`
- Pass the same feature-gate configuration to both binaries through Helm and reject invalid gate combinations during rendering.
- Mount job-extension timeout configuration and expose Pod reclaim through Helm. Configuration changes restart the job-extension Deployment automatically.
- Build and test both binaries with `GOWORK=off` without modifying `go.mod` or generating a vendor tree.
- Verify canonical, Helm, and envtest CRDs from a pinned controller-gen version.
- Gate branch and release image builds on lint, unit, integration, build, Helm, and CRD validation.
- Build `linux/amd64` and `linux/arm64` images for GHCR and the Beijing and Hangzhou Aliyun registries.

### Upgrade notes

- Apply the updated QueueUnit CRD before starting v1.9.0 controllers. The bundled Helm chart installs the synchronized CRD.
- Alpha lifecycle features remain disabled by default. Enable the same gates in both `koord-queue` and `koord-queue-controllers`; the Helm chart handles this through the shared `featureGates` value.
- `MaximumExecutionTime` requires both `QueueUnitActive` and `QueueUnitConditions`. `QueueUnitRequeueState` requires `QueueUnitConditions`.
- Pod reclaim remains disabled by default and can be enabled with `extension.jobextensions.enablePodReclaim`.
- Koordinator v1.8.0 is the currently validated integration baseline. Validate against Koordinator v1.9.0 before declaring that version combination supported.

## v0.1.0
### Features
- Build a framework to support different queueing policies and quota systems
- Support two basic queueing policies for the elements in single queue: FIFO、Priority
- Support dynamic adjustment of job priority in queue
- Integrate with the ResourceQuota in Kubernetes
- Integrate with TFJob, PyTorchJob for distribute deep learning training
- Integrate with ET-operator for elastic training 
