---
name: build-and-deploy
description: Build, test, release, and deploy the koord-queue (kube-queue) project. Covers compiling the koord-queue and koord-queue-controllers binaries, running unit/integration tests, building and publishing multi-arch Docker images via Git tags, installing the Helm chart, and submitting queued jobs. Use when the user asks how to build, compile, test, release, tag, ship images, deploy, install, or run/use koord-queue / kube-queue.
---

# koord-queue: Build, Release & Usage

koord-queue (repo dir `kube-queue`, Go module `github.com/koordinator-sh/koord-queue`) is native job
queuing for Koordinator. It integrates with ElasticQuota to enforce resource fairness and supports
FIFO/Priority queue policies. Two binaries ship from this repo:

| Binary | Source | Role | Image / Dockerfile |
|--------|--------|------|--------------------|
| `koord-queue` | `cmd/main.go` | Queue controller + scheduler; serves metrics on `:10259` | `koord-queue` / `Dockerfile` |
| `koord-queue-controllers` | `cmd/controllers/main.go` | Job-extension, admission & reservation controllers | `koord-queue-controllers` / `Dockerfile.controllers` |

**Go version: 1.26.0** (`go.mod`). The repo has a `go.work`; CI and clean builds set `GOWORK=off`
to avoid pulling sibling workspace modules — do the same locally when a build behaves unexpectedly.

---

## Build

**Primary (Makefile):**
```bash
make build          # == make build-queue: runs fixcodec, then builds bin/koord-queue
make build-queue    # GOOS/GOARCH-aware build of cmd/main.go -> bin/koord-queue
make clean          # rm -rf ./bin
```
`build-queue` first runs `make fixcodec` (`hack/fix-codec-factory.sh`) — required codegen fixup, do
not skip it.

**CI-style build (matches `.github/workflows/ci.yaml`):**
```bash
GOWORK=off go build -o bin/koord-queue ./cmd
GOWORK=off go build -o bin/koord-queue-controllers ./cmd/controllers
```

**Cross-compile:** the Makefile honors `TARGETOS` / `TARGETARCH` (default: host `uname`). Example:
```bash
TARGETOS=linux TARGETARCH=amd64 make build-queue
```

**Docker images (built locally):**
```bash
docker build -t koord-queue:dev -f Dockerfile .
docker build -t koord-queue-controllers:dev -f Dockerfile.controllers .
```
Both use `golang:1.26-alpine` (multi-stage build) → `alpine:3.16` runtime.

---

## Test

```bash
make unit-test          # hack/unit-test.sh -> go test -mod=vendor ./...  (runs fixcodec + update-vendor first)
make integration-test   # sets up envtest (K8s 1.33) then runs pkg/**/test/integration/...
make setup-envtest      # download kubebuilder assets for the ENVTEST_K8S_VERSION (1.33) into bin/k8s
```

`integration-test` requires the envtest control plane (etcd + kube-apiserver). The Makefile target
installs `setup-envtest`, downloads assets, exports `KUBEBUILDER_ASSETS`, then runs
`./pkg/jobext/test/integration/... ./pkg/test/integration/...`. Run a single integration package with
`hack/integration-test.sh <extra go-test args>` (e.g. `-run TestName -v`).

**CI mirror** (`.github/workflows/ci.yaml`) runs four jobs: `golangci-lint`, `unit-tests`
(`GOWORK=off go test -mod=mod ./pkg/controller/... ./pkg/queue/... ./pkg/controllers/...`), `build`,
and `integration-tests` (each integration package in its own `go test` process so each envtest control
plane is torn down before the next starts).

For regenerating code/CRDs/vendor before building, see the hack-scripts table in
[reference.md](reference.md).

---

## Release (Docker images)

Releases are driven by **Git tags**; GitHub Actions does the multi-arch build & push.

**To cut a release:**
```bash
git tag v1.22.3
git push origin v1.22.3     # tag matching 'v*' triggers .github/workflows/release.yml
```

`release.yml` builds `linux/amd64,linux/arm64` for **both** targets and pushes each to three registries:

- `ghcr.io/<owner>/<target>:<tag>`
- `registry.cn-beijing.aliyuncs.com/<owner>/<target>:<tag>`
- `registry.cn-hangzhou.aliyuncs.com/<owner>/<target>:<tag>`

Pushes to `main` / `release-*` (not tags) trigger `build-images.yml`, publishing `:latest` and
`:<sha>` tags to the same registries. Required repo secrets: `ALIYUN_USERNAME`, `ALIYUN_PWD`
(`GITHUB_TOKEN` is provided automatically).

> **Internal pre-env release** (Chorus image build + application-catalog version bump + MR + pre
> pipeline) is a *separate* flow handled by the `kube-queue-release` skill — use that skill when the
> user asks to "release to pre" / "发布到预发", not this repo-native tag flow.

**Chart version:** bump `charts/v1.2.0/Chart.yaml` (`version` + `appVersion`) and the image `tag`
values in `charts/v1.2.0/values.yaml` when publishing a new deployable version. Record user-facing
changes in `charts/v1.2.0/README.md`'s Release Note table.

---

## Deploy (Helm)

The chart lives in `charts/v1.2.0/` (chart/appVersion `1.22.2`) and installs into the `koord-queue`
namespace: the `koord-queue-controller` Deployment, the `job-extensions` Deployment, CRDs
(Queue, QueueUnit), the AdmissionCheck, APIServices, plugin config, and RBAC.

```bash
helm install koord-queue ./charts/v1.2.0 -n koord-queue --create-namespace
# override images / options:
helm install koord-queue ./charts/v1.2.0 -n koord-queue --create-namespace \
  --set controller.image.repository=<reg>/koord-queue --set controller.image.tag=v1.22.3 \
  --set extension.jobextensions.repository=<reg>/koord-queue-controllers \
  --set extension.jobextensions.tag=v1.22.3 \
  --set extension.tf.enable=true --set extension.pytorch.enable=true
```

Key `values.yaml` knobs (full table in [reference.md](reference.md)):
- `global.imagePrefix` — registry prefix prepended to image repositories (default `ghcr.io`).
- `controller.queueGroupPlugin` — quota grouping plugin (default `elasticquotav2`).
- `controller.enableBlockingMode`, `enableStrictPriority`, `enableVisibilityServer`,
  `enableResourceCheckWithScheduler`, `defaultPreemptible`.
- `extension.<type>.enable` — turn on per job type: `batchjob` (default on), `tf`, `pytorch`, `spark`,
  `ray`, `argo`, `mpi`. Enabled types are passed as `--enabled-extensions` to `koord-queue-controllers`.
- `oversellrate`, `qps`, `burst`.

CRDs alone (without Helm) can be applied from `pkg/crd/scheduling.x-k8s.io_queues.yaml` and
`pkg/crd/scheduling.x-k8s.io_queueunits.yaml`.

---

## Usage (submitting queued work)

Resources are grouped under API group `scheduling.x-k8s.io/v1alpha1`.

**1. Define a Queue** with a policy (`FIFO` or `Priority`):
```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: Queue
metadata: { name: queue1, namespace: queue1 }
spec:
  queuePolicy: Priority
```

**2. Provide quota** — a namespace `ResourceQuota` (or an ElasticQuotaTree) bounds what the queue can
admit. With ElasticQuotaTree, `kube-queue/`-prefixed annotations sync to the Queue, and the resource
name `koord-queue/max-jobs` caps the number of concurrently dequeued jobs.
```yaml
apiVersion: v1
kind: ResourceQuota
metadata: { name: default }
spec: { hard: { cpu: "4", memory: 4Gi } }
```

**3. Submit work.** With job extensions enabled, submit the native job (TFJob, PyTorchJob, MPIJob,
SparkApplication, native `batch/v1` Job, Argo Workflow, RayJob) and the controllers create the
`QueueUnit` automatically. To enqueue manually, create a QueueUnit referencing the job:
```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: QueueUnit
metadata: { name: unit1, namespace: default }
spec:
  consumerRef:              # the job this unit represents
    apiVersion: kubeflow.org/v1
    kind: TFJob
    name: job1
    namespace: default
  queue: default            # target queue
  priority: 100
  priorityClassName: high-priority
  resource: { cpu: 3, memory: 3Gi }   # job's total resource request
```

Runnable manifests: `examples/tfjob/` (Queue + ResourceQuota + QueueUnit + jobs),
`examples/multiqueues/` (multi-queue fairness), `examples/tf-operator/` (operator install).

**Inspect:**
```bash
kubectl get queue -A
kubectl get queueunit -A -o wide     # shows queue, priority, phase
kubectl -n koord-queue logs deploy/koord-queue-controller
kubectl -n koord-queue logs deploy/job-extensions
```

---

## Deeper references

- [reference.md](reference.md) — full `koord-queue` / `koord-queue-controllers` CLI flags, complete Helm
  `values.yaml` table, `hack/` codegen scripts, and CRD/API field notes.
- Repo docs: `doc/queue.md`, `doc/queueunit.md`, `doc/design.md`, `doc/preemption.md`,
  `doc/elasticquota-queue-mapping.md`.
