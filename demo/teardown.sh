#!/usr/bin/env bash
# Deletes the demo clusters and their kubeconfig.
set -euo pipefail
cd "$(dirname "$0")"
kind delete cluster --name kh-shop --kubeconfig "$PWD/kubeconfig" || true
kind delete cluster --name kh-payments --kubeconfig "$PWD/kubeconfig" || true
rm -f kubeconfig
