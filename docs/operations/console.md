# RustQueue Console

RustQueue Console is an observation surface for one `RustQueue` object in one
Kubernetes namespace. The Helm chart enables one Console Deployment and one
ClusterIP Service by default. Topic and Channel management is a separate,
default-off capability.

## Data and security boundary

The browser only talks to `rustqueue-console`. It never receives Kubernetes
credentials or broker tokens. The backend uses a namespace-scoped ServiceAccount
to read:

- the selected RustQueue CR;
- broker Pods and PVCs;
- related Kubernetes Events.

The backend mounts `console-token` and calls each broker's `GET /v1/observe`
endpoint. The endpoint contains health, counters, latency histograms, topic and
channel metadata, disk state and compatibility metadata. It does not contain a
message body or an API capable of reading one. The console token is distinct
from `admin-token` and cannot authorize drain, scrub, upgrade or the
NSQ-compatible management endpoints. The browser never receives this token.

Console does not read container logs or Kubernetes Secrets. It also has no
built-in user login. The Service remains `ClusterIP`; apply authentication at
your existing VPN, SSO or Ingress boundary if users access it outside the
cluster network.

## Pages

- Overview: readiness, live rates, backlog, disk, anomalies and current work.
- Brokers: Pod, PVC, image, version, capability, connections and disk state.
- Topics: owners, channels, depth, in-flight, deferred and ACK gap state, plus
  optional native Topic/Channel management.
- Storage: capacity, watermarks, segments, critical latency, scrub and GC.
- Operations: Conditions, current operation, history and Kubernetes Events.
- Configuration: the effective RustQueue CR spec, excluding all Secrets.

The interface follows the browser language and operating-system theme on first
use. Both can be overridden from the header and are stored locally in the
browser. Chinese and English are supported.

## Live trends

The backend polls a lightweight runtime/revision head every two seconds and
retains 15 minutes of samples in a bounded in-memory buffer. The full
Topic/Channel catalog is cached and refreshed on revision, Pod or management
fence changes, plus a 30-second fallback. There is no Prometheus dependency.
The buffer resets on Console Pod restart and rate calculation resets whenever
the observed broker membership changes.

```yaml
console:
  enabled: true
  pollIntervalSeconds: 2
  catalogRefreshIntervalSeconds: 30
  historyMinutes: 15
```

The poll interval must be 1 to 5 seconds. Catalog refresh must be between the
poll interval and 300 seconds. The history window must be 1 to 60 minutes.

## Topic and Channel management

Management is Kubernetes-only and remains disabled unless explicitly enabled:

```sh
helm upgrade rustqueue deploy/helm/rustqueue \
  --namespace rustqueue \
  --set console.management.enabled=true
```

Enabling it activates the Console write routes, the broker's narrow native
management routes and namespace-scoped RBAC for `RustQueueTopic`,
`RustQueueChannel` and audit Events. Standalone Console deployments remain
read-only. On an existing cluster this changes broker configuration and follows
the normal drain-aware rollout. A one-broker cluster cannot perform that rollout
safely, so enable management on its initial install or add a second broker first.

The ClusterIP network boundary is the user authentication boundary. Management
starts locked in the browser. An operator must type the exact
`namespace/queue` value to unlock it for 30 minutes; refreshes do not extend the
session. Every mutation also requires same-origin JSON, an HttpOnly session
cookie, a CSRF token and a one-time 60-second action token bound to the resource
UID, resource version, action and current owners.

Durable Topics and Channels support create, pause, unpause, empty and delete.
Ephemeral Channels are observation-only. Empty and delete show an impact
preview and require the exact resource name. Deletion first persists a
tombstone, then synchronizes the fence to all brokers before removing data.
The default tombstone lifetime is 10 minutes and can be changed with
`console.management.tombstoneSeconds`.

The Console selects a healthy, non-maintenance, disk-eligible broker for new
Topics. A mutation first persists an operation ID and owner progress in the
control CRD, then the reconciler applies one owner at a time. A Console restart
continues at the first unfinished owner, and the broker durably deduplicates a
replayed operation ID. During a visible multi-owner migration, empty and delete
fail closed. Registry outages, stale CRDs and unhealthy owners are retried;
non-retryable state drift is shown as `FAILED` and can be resumed with the UI
Retry action without repeating owners already recorded as complete.

Successful and failed actions are written to structured Console logs and
Kubernetes Events. The audit record includes source IP, user agent, action and
result; it never includes a message body or broker token.

## Access

For local inspection:

```sh
kubectl -n rustqueue port-forward svc/rustqueue-console 4180:4180
```

Then open `http://127.0.0.1:4180`. To disable the component:

```sh
helm upgrade rustqueue deploy/helm/rustqueue \
  --namespace rustqueue \
  --set console.enabled=false
```
