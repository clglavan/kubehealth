#!/usr/bin/env bash
# Builds the kh-bench kind cluster with the incidents in bench/incidents/*,
# for recording bench snapshots (see "Benchmark" in docs/reference.md).
#
#   KUBECONFIG=/path/kh-bench.kubeconfig bench/incidents/setup.sh
#
# Then wait until the failures have settled (crash loops at >= 3 restarts,
# CronJobs run >= 2 times: about 8 minutes) and record, one snapshot per
# namespace group, within the hour (Kubernetes keeps events for an hour):
#
#   kubehealth bench record -kubeconfig $KUBECONFIG -context kh-bench \
#     -skip-namespaces kube-system,kube-public,kube-node-lease,local-path-storage,<the other groups> \
#     -out bench/snapshots/bench-a
#
# Groups: bench-a = tidepool, tidepool-ingest; bench-b = larkspur-books,
# larkspur-notify; bench-c = quillmoss-render, quillmoss-sandbox;
# bench-d = quillmoss-ops.
#
# Fake applications are busybox scripts (incidents/*/app/<binary>, first
# comment line "# image: <ref> [<ref> …]") baked into local images and loaded
# into the nodes, so describe shows ordinary images and arguments.
set -euo pipefail
cd "$(dirname "$0")"
: "${KUBECONFIG:?set KUBECONFIG to the kh-bench kubeconfig}"
export KUBECONFIG
name=kh-bench
kind=${KIND:-kind}

if ! "$kind" get clusters 2>/dev/null | grep -qx "$name"; then
  cat >"$KUBECONFIG.kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF
  "$kind" create cluster --name "$name" --kubeconfig "$KUBECONFIG" --config "$KUBECONFIG.kind.yaml"
  kubectl config rename-context "kind-$name" "$name" >/dev/null
fi
kubectl config use-context "$name" >/dev/null
kubectl wait --for=condition=Ready node --all --timeout=180s >/dev/null

echo "Node pools: $name-worker2 is the tainted reporting pool"
kubectl label node "$name-worker" node-pool=general --overwrite >/dev/null
kubectl label node "$name-worker2" node-pool=reporting --overwrite >/dev/null
kubectl taint node "$name-worker2" dedicated=reporting:NoSchedule --overwrite >/dev/null

echo "Building the fake application images"
docker pull -q busybox:1.36 >/dev/null
refs=()
for script in */app/*; do
  bin=$(basename "$script")
  for ref in $(sed -n 's/^# image: //p' "$script"); do
    docker build -q -t "$ref" -f - "$(dirname "$script")" >/dev/null <<EOF
FROM busybox:1.36
COPY $bin /usr/local/bin/$bin
RUN chmod 755 /usr/local/bin/$bin
ENTRYPOINT ["/usr/local/bin/$bin"]
EOF
    refs+=("$ref")
  done
done
"$kind" load docker-image --name "$name" "${refs[@]}" >/dev/null 2>&1

# KubeHealth links problems in different namespaces that start within 10 s
# of each other into one pattern row; the overview note is about the top
# row. So the second namespace of every snapshot group starts later.
wave2='namespace: (tidepool-ingest|larkspur-notify|quillmoss-sandbox)[,}]'
apply_wave() { # 1 | 2
  local m
  for m in */manifest.yaml; do
    if grep -Eq "$wave2" "$m"; then [[ $1 == 2 ]] || continue; else [[ $1 == 1 ]] || continue; fi
    case "$m" in
      healthy-reporting-node/*) ;; # node labels and taint above
      vellum-gateway-stuck-rollout/*)
        sed -e 's/@VERSION@/3.4.0/' -e 's/@LISTEN@/:8080/' "$m" | kubectl apply -f - >/dev/null ;;
      healthy-tally-reindex/*)
        # The first run finds the index lock held and exits 1; a retry succeeds.
        sed "s/@LOCK_HELD_UNTIL@/$(( $(date +%s) + 20 ))/" "$m" | kubectl apply -f - >/dev/null ;;
      *)
        kubectl apply -f "$m" >/dev/null ;;
    esac
  done
}

echo "Applying the incidents: tidepool, larkspur-books, quillmoss-render, quillmoss-ops"
start=$(date +%s)
apply_wave 1
echo "Rollouts: a restart and a broken release"
kubectl -n tidepool rollout status deploy/reef-catalog --timeout=180s >/dev/null
kubectl -n tidepool rollout restart deploy/reef-catalog >/dev/null
kubectl -n quillmoss-render rollout status deploy/vellum-gateway --timeout=180s >/dev/null
sed -e 's/@VERSION@/3.5.0/' -e 's/@LISTEN@/:80800/' vellum-gateway-stuck-rollout/manifest.yaml | kubectl apply -f - >/dev/null
while (( $(date +%s) < start + 150 )); do sleep 5; done

echo "Applying the incidents: tidepool-ingest, larkspur-notify, quillmoss-sandbox"
apply_wave 2
echo "Rollouts: a scale-down"
kubectl -n larkspur-notify rollout status deploy/notify-fanout --timeout=180s >/dev/null
kubectl -n larkspur-notify scale deploy/notify-fanout --replicas=1 >/dev/null

echo "Done. Wait ~8 minutes before recording."
