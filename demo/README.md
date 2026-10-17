# Demo clusters

Two local [kind](https://kind.sigs.k8s.io) clusters with realistic failures, for trying KubeHealth without a real cluster. The screenshots and the video in the main README were taken from them.

```bash
./demo/setup.sh                        # about 3 minutes; needs Docker (or colima) and kind
./kubehealth -kubeconfig demo/kubeconfig
./demo/teardown.sh                     # when you're done
```

The scripts write their own `demo/kubeconfig`; your `~/.kube/config` is not touched.

## What's broken

**shop-staging** (three nodes; the name gives it the `STAGE` tag)

| Namespace | What happens | What KubeHealth shows |
|---|---|---|
| `orders` | `orders-db` asks for the StorageClass `fast-ssd`, which doesn't exist, so its volume is never created and the database never starts. `orders-worker` can't resolve the database and crash-loops. | Ongoing problems for the worker and the database, a new one for the volume claim; the worker's logs say why |
| `catalog` | The `catalog-sync` CronJob fails every minute (its supplier token expired) | A chronic CronJob problem: last success 3 days ago |
| `search` | `search-indexer` runs out of its 48 Mi memory limit | Exit code 137 in the container state; the AI note suggests raising the limit |
| `checkout` | A release of `checkout-api` with a typo in the image tag; the old pods keep serving | ImagePullBackOff on the new ReplicaSet only |
| `frontend` | `storefront`'s readiness probe asks for `/healthz`, which nginx doesn't serve | A chronic problem: 0 of 2 ready |
| `checkout`, `orders`, `catalog` | `log-shipper` deployed to all three at once from a registry that doesn't resolve | One pattern row: *log-shipper in 3 namespaces* |
| cluster-wide | A released PersistentVolume whose reclaim policy can't be carried out | A cluster-wide problem and a failed volume in the status |
| `monitoring` | Healthy DaemonSet and CronJob | Normal activity only |

**payments-prod** (one node; the name makes it production, with the red stripe)

| Namespace | What happens |
|---|---|
| `payments` | `fraud-scorer` needs the ConfigMap `fraud-rules-v7`, which was never created |

## Backdated events

Kubernetes keeps events for an hour, so on a fresh cluster every problem is *new*. To show the *ongoing* and *chronic* tiers, `setup.sh` adds three Warning events with an older `firstTimestamp` (as if from Pods that have since been replaced) and moves `catalog-sync`'s last success three days back. These are the only made-up data; everything else is the cluster reacting to the manifests.

They expire after an hour like any event: run `./demo/setup.sh --backdate` to add them again, or pass `--no-backdate` to setup to skip them.

## If a cluster's kube-proxy crash-loops

Two kind clusters in one Docker VM can run out of inotify instances. Raise the limits in the VM, for example on colima:

```bash
colima ssh -- sudo sysctl -w fs.inotify.max_user_watches=524288 fs.inotify.max_user_instances=512
```
