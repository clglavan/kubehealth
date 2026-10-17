#!/usr/bin/env bash
# Builds two kind clusters with realistic failures, for trying KubeHealth
# without a real cluster. Everything goes into demo/kubeconfig; your
# ~/.kube/config is not touched.
#
#   ./demo/setup.sh                 create the clusters and break them
#   ./demo/setup.sh --no-backdate   same, but leave every problem "new"
#   ./demo/setup.sh --backdate      only refresh the backdated events (they expire after an hour)
#
# Then: ./kubehealth -kubeconfig demo/kubeconfig
set -euo pipefail
cd "$(dirname "$0")"
export KUBECONFIG="$PWD/kubeconfig"

mode="${1:-}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# A freshly created cluster's API server can time out for a while; retry.
retry() {
  local n
  for n in 1 2 3 4 5 6; do
    "$@" && return 0
    echo "  retrying in 5 s: $*" >&2
    sleep 5
  done
  return 1
}
shop() { retry kubectl --context shop-staging "$@"; }

# RFC 3339 time, $1 seconds ago (GNU or BSD date).
ago() {
  local t=$(( $(date +%s) - $1 ))
  date -u -d "@$t" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$t" +%Y-%m-%dT%H:%M:%SZ
}

# Kubernetes keeps events for an hour, so a fresh cluster only has new
# problems. To show the ongoing and chronic tiers, add one older event to two
# workloads (as if from a Pod that has since been replaced) and move the
# CronJob's last success three days back.
backdate() {
  local worker_rs storefront_rs
  worker_rs=$(shop -n orders get rs -l app=orders-worker -o jsonpath='{.items[0].metadata.name}')
  storefront_rs=$(shop -n frontend get rs -l app=storefront -o jsonpath='{.items[0].metadata.name}')
  shop -n catalog patch cronjob catalog-sync --subresource=status --type=merge \
    -p "{\"status\":{\"lastSuccessfulTime\":\"$(ago 259200)\"}}" >/dev/null
  shop -n orders delete event demo-aged-worker demo-aged-db --ignore-not-found >/dev/null
  shop -n frontend delete event demo-aged-storefront --ignore-not-found >/dev/null
  cat >"$tmp/aged.yaml" <<EOF
apiVersion: v1
kind: Event
metadata: { name: demo-aged-worker, namespace: orders }
involvedObject: { apiVersion: v1, kind: Pod, namespace: orders, name: $worker_rs-9xk2m, fieldPath: "spec.containers{worker}" }
type: Warning
reason: BackOff
message: Back-off restarting failed container worker in pod $worker_rs-9xk2m_orders
count: 64
firstTimestamp: "$(ago 18000)"
lastTimestamp: "$(ago 2100)"
source: { component: kubelet }
---
apiVersion: v1
kind: Event
metadata: { name: demo-aged-db, namespace: orders }
involvedObject: { apiVersion: v1, kind: Pod, namespace: orders, name: orders-db-0 }
type: Warning
reason: FailedScheduling
message: "0/3 nodes are available: pod has unbound immediate PersistentVolumeClaims. preemption: 0/3 nodes are available: 3 Preemption is not helpful for scheduling."
count: 58
firstTimestamp: "$(ago 18000)"
lastTimestamp: "$(ago 2100)"
source: { component: default-scheduler }
---
apiVersion: v1
kind: Event
metadata: { name: demo-aged-storefront, namespace: frontend }
involvedObject: { apiVersion: v1, kind: Pod, namespace: frontend, name: $storefront_rs-q8w2d, fieldPath: "spec.containers{web}" }
type: Warning
reason: Unhealthy
message: "Readiness probe failed: HTTP probe failed with statuscode: 404"
count: 16934
firstTimestamp: "$(ago 172800)"
lastTimestamp: "$(ago 3000)"
source: { component: kubelet }
EOF
  shop apply -f "$tmp/aged.yaml" >/dev/null
  echo "Backdated events added; they expire in an hour (rerun with --backdate)."
}

if [[ "$mode" == "--backdate" ]]; then
  backdate
  exit 0
fi

# Creates the kind cluster unless it exists, and names its context.
ensure_cluster() {
  local name=$1 ctx=$2
  shift 2
  if ! kind get clusters 2>/dev/null | grep -qx "$name"; then
    kind create cluster --name "$name" --kubeconfig "$KUBECONFIG" "$@"
  elif ! kubectl config get-contexts -o name 2>/dev/null | grep -qx "$ctx"; then
    kind export kubeconfig --name "$name" --kubeconfig "$KUBECONFIG"
  fi
  if ! kubectl config get-contexts -o name | grep -qx "$ctx"; then
    kubectl config rename-context "kind-$name" "$ctx" >/dev/null
  fi
}

echo "Creating clusters (kubeconfig: $KUBECONFIG)"
ensure_cluster kh-shop shop-staging --config kind-shop.yaml
ensure_cluster kh-payments payments-prod
kubectl config use-context shop-staging >/dev/null

echo "Deploying the shop and the payments service"
shop apply -f shop.yaml >/dev/null
retry kubectl --context payments-prod apply -f payments.yaml >/dev/null
for d in checkout/checkout-api checkout/payment-gateway orders/orders-api catalog/catalog-api frontend/cdn-proxy; do
  shop -n "${d%/*}" rollout status "deploy/${d#*/}" --timeout=300s >/dev/null
done

echo "Breaking things"
# One workload rolled out to three namespaces from a registry that doesn't resolve.
for ns in checkout orders catalog; do
  cat >>"$tmp/log-shipper.yaml" <<EOF
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: log-shipper, namespace: $ns, labels: { app: log-shipper } }
spec:
  replicas: 1
  selector: { matchLabels: { app: log-shipper } }
  template:
    metadata: { labels: { app: log-shipper } }
    spec:
      containers:
        - name: shipper
          image: registry.acme-demo.internal/platform/log-shipper:3.1.0
          resources: { requests: { cpu: 5m, memory: 8Mi } }
EOF
done
shop apply -f "$tmp/log-shipper.yaml" >/dev/null
# More than 10 s later, so it isn't merged into the pattern above: a release with a typo in the tag.
sleep 15
shop -n checkout set image deploy/checkout-api api=nginx:1.27.9-alpine >/dev/null
# Release a volume whose reclaim policy can't be carried out (a cluster-wide warning).
shop -n monitoring delete pvc reports-archive --ignore-not-found --wait=false >/dev/null

if [[ "$mode" != "--no-backdate" ]]; then
  backdate
fi

cat <<'EOF'

Done. Start KubeHealth on the demo clusters:

  ./kubehealth -kubeconfig demo/kubeconfig

Remove everything with ./demo/teardown.sh
EOF
