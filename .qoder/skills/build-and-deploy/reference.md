# koord-queue reference

Detailed reference for the `build-and-deploy` skill. Values are taken from the repo at authoring time;
verify against source before relying on exact defaults.

## `koord-queue` CLI flags

Defined in `cmd/app/options/options.go` (`ServerOption.AddFlags`). `cmd/main.go` also wires
`klog` flags, queue-policy flags (`pkg/queue/queuepolicies`), and exposes `/metrics` + pprof on
`:10259`.

| Flag | Default | Meaning |
|------|---------|---------|
| `--kubeconfig` | `""` | Path to kubeconfig (in-cluster config if empty) |
| `--qps` | `50` | Max client QPS to the apiserver |
| `--burst` | `50` | Max client burst |
| `--podInitialBackoffSeconds` | `1` | Initial backoff for pods in the backoff queue |
| `--podMaxBackoffSeconds` | `20` | Max backoff for pods in the backoff queue |
| `--oversellrate` | `1` | Resource oversell rate |
| `--scheduleSuspendTimeInMillSecond` | `10` | Suspend time (ms) during scheduling |
| `--enableApiHandler` | `false` | Enable the API handler |
| `--defaultPreemptible` | `true` | Default preemptible setting |
| `--leaderElection` | `true` | Enable leader election |
| `--enableParentLimit` | `false` | Enable parent quota limit |
| `--enableVisibilityServer` | `true` | Enable the visibility server |
| `--feature-gates` | `""` | `key=value` pairs toggling alpha/experimental features |
| `--admissionCheckControllerWorker` | `2` | Workers checking AdmissionCheckState in QueueUnit status |
| `--config` | `""` | Path to the plugin config file |
| `--strictDequeueMode` | `false` | QueueUnits become SchedReady after dequeue and wait for the scheduler to mark SchedSucceed/SchedFailed |
| `--queue-list` | `""` | Only enable strictDequeueMode for the listed queues |

## `koord-queue-controllers` CLI flags

Defined in `cmd/controllers/main.go` (`ControllerOptions`).

| Flag | Default | Meaning |
|------|---------|---------|
| `--qps` | `300` | Client QPS to the apiserver |
| `--burst` | `300` | Client burst |
| `--metrics-bind-address` | `:8080` | Metrics endpoint bind address |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe bind address (`/healthz`, `/readyz`) |
| `--leader-elect` | `false` | Enable leader election for the controller manager |
| `--leader-namespace` | `koord-queue` | Namespace for the leader-election resource |
| `--enable-job-extensions` | `true` | Enable job-extension controllers |
| `--enabled-extensions` | `job` | Comma-separated job types to enable (see registry keys below) |
| `--manage-all-jobs` | `false` | Manage all jobs, not only those submitted with `suspend` |
| `--config` | `""` | Path to the arguments/config file (YAML) |
| `--workers` | `20` | Workers per controller |
| `--wqqps` | `100` | Work-queue QPS |
| `--enable-pod-reclaim` | `false` | Enable pod reclaim |
| `--default-requeue-period` | `1m` | Default requeue period |
| `--enable-reservation` | `false` | Enable the reservation controller |
| `--enable-network-aware` | `false` | Enable network-aware scheduling (internal) |

The leader-election ID becomes `koord-queue-controllers-<extensions joined by ->` when job extensions
are enabled, so different `--enabled-extensions` sets can run as separate leader-elected deployments.

**`--enabled-extensions` registry keys** (Helm maps `extension.<type>.enable` → these tokens):
`job` (native `batch/v1` Job, from `batchjob`), `sparkapp`, `rayjob`, `tfjob`, `pytorchjob`,
`workflow` (Argo). Unknown tokens are skipped with a log line.

## Helm `values.yaml`

Chart: `charts/v1.2.0/` (chart & app version `1.22.2`).

| Path | Default | Purpose |
|------|---------|---------|
| `global.imagePrefix` | `ghcr.io` | Registry prefix prepended to every image repository |
| `tolerations` | `[]` | Pod tolerations for both deployments |
| `isNeedNotebookExtension` | `false` | Enable notebook extension |
| `oversellrate` | `1` | Passed as `--oversellrate` to `koord-queue` |
| `qps` / `burst` | `50` / `50` | Passed to `koord-queue` |
| `jobRunningTimeout` | `"0"` | Job running timeout (`0` = disabled) |
| `jobBackoffTime` | `"0"` | Job requeue backoff (`0` = disabled) |
| `installation.namespace` | `koord-queue` | Install namespace |
| `installation.roleListGroups` | `['*']` | API groups granted in the RBAC role |
| `controller.image.repository` | `koordinatorsh/koord-queue` | `koord-queue` image repo (prefixed by `global.imagePrefix`) |
| `controller.image.tag` | `latest` | `koord-queue` image tag |
| `controller.queueGroupPlugin` | `elasticquotav2` | Quota grouping plugin (sets `QueueGroupPlugin` env) |
| `controller.enableBlockingMode` | `false` | Sets `StrictPriority=true` env (queue blocking) |
| `controller.enableStrictPriority` | `false` | Sets `StrictConsistency=true` env |
| `controller.enableVisibilityServer` | `false` | `--enableVisibilityServer` |
| `controller.enableResourceCheckWithScheduler` | `false` | `--strictDequeueMode` |
| `controller.defaultPreemptible` | `true` | Default preemptible |
| `controller.resources` | cpu 100m–200m / mem 256Mi–512Mi | Controller container resources |
| `extension.jobextensions.repository` | `koordinatorsh/jobextensions` | `koord-queue-controllers` image repo |
| `extension.jobextensions.tag` | `latest` | Job-extensions image tag |
| `extension.jobextensions.resources` | mem limit 2Gi | Job-extensions container resources |
| `extension.batchjob.enable` | `true` | Enable native Job (`job`) extension |
| `extension.tf.enable` | `false` | Enable TFJob (`tfjob`) |
| `extension.pytorch.enable` | `false` | Enable PyTorchJob (`pytorchjob`) |
| `extension.spark.enable` | `false` | Enable SparkApplication (`sparkapp`) |
| `extension.ray.enable` | `false` | Enable RayJob (`rayjob`) |
| `extension.argo.enable` | `false` | Enable Argo Workflow (`workflow`) |
| `extension.mpi.enable` | `false` | Enable MPIJob |
| `pluginConfigs` | Priority + ElasticQuotaV2 | Rendered into the plugin config ConfigMap mounted at `/etc/kubernetes/ack-config/plugin.yaml` |

The rendered `koord-queue-controller` Deployment runs `koord-queue --v=4 --oversellrate=… --qps=… --burst=…
--config=/etc/kubernetes/ack-config/plugin.yaml --enableVisibilityServer=… --strictDequeueMode=…`. The
`job-extensions` Deployment runs `/usr/bin/koord-queue-controllers --leader-elect
--enabled-extensions=<derived from extension.*.enable>`.

## `hack/` scripts

| Script | Make target | Purpose |
|--------|-------------|---------|
| `hack/fix-codec-factory.sh` | `fixcodec` | Codec factory fixup; runs before every build/test |
| `hack/update-vendor.sh` | `update-vendor` | Refresh the vendor directory |
| `hack/unit-test.sh` | `unit-test` | `go test -mod=vendor ./...` |
| `hack/integration-test.sh` | `integration-test` | Install setup-envtest, download K8s `${ENVTEST_K8S_VERSION:-1.33}` assets, run integration packages (accepts extra `go test` args) |
| `hack/update-crd.sh` | — | `controller-gen crd paths=./pkg/apis/... output:crd:dir=./pkg/crd` |
| `hack/update-internal-api.sh` | — | Regenerate internal API code |
| `hack/update-koord-queue-api.sh` | — | Sync the vendored kubequeue-api |

`ENVTEST_K8S_VERSION` is `1.33` (Makefile + `hack/integration-test.sh`). envtest binaries land in
`bin/k8s/`.

## CRDs & API

- API group/version: `scheduling.x-k8s.io/v1alpha1`; kinds `Queue` and `QueueUnit`.
- CRD manifests: `pkg/crd/scheduling.x-k8s.io_queues.yaml`, `pkg/crd/scheduling.x-k8s.io_queueunits.yaml`
  (regenerate with `hack/update-crd.sh`).

**Queue** — `spec.queuePolicy` selects `FIFO` or `Priority`. Queues map to quota via
ElasticQuota / ElasticQuotaTree; `kube-queue/`-prefixed annotations on the tree sync to the Queue
object, and the special resource `koord-queue/max-jobs` limits concurrently dequeued jobs.

**QueueUnit** — one queued job. Key `spec` fields: `consumerRef` (apiVersion/kind/name/namespace of the
owning job), `queue`, `priority`, `priorityClassName`, `resource` (aggregate request). Job extensions
create/maintain QueueUnits automatically for enabled job types; `status` tracks the lifecycle phase and
`admissions`.

## CI workflows (`.github/workflows/`)

| Workflow | Trigger | Effect |
|----------|---------|--------|
| `ci.yaml` | push to `main`/`release-*`, PRs, manual | golangci-lint, unit tests, build, per-package integration tests (`GOWORK=off`) |
| `build-images.yml` | push to `main`/`release-*`, PRs (build only), manual | Multi-arch images → GHCR + Aliyun BJ/HZ tagged `:latest` and `:<sha>` (push skipped for PRs) |
| `release.yml` | push tag `v*` | Multi-arch images for both targets → GHCR + Aliyun BJ/HZ tagged `:<tag>` |

Registries: `ghcr.io`, `registry.cn-beijing.aliyuncs.com`, `registry.cn-hangzhou.aliyuncs.com`.
Secrets required for pushes: `ALIYUN_USERNAME`, `ALIYUN_PWD`.
