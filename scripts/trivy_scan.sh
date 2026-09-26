#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/runpath_guard.sh"

set -euo pipefail

trivy_image="${TRIVY_IMAGE:-aquasec/trivy:0.74.0}"
cache_dir="${TRIVY_CACHE_DIR:-$HOME/.cache/trivy}"
html_report=""

usage() {
  cat <<'USAGE'
Usage: ./scripts/trivy_scan.sh [--html] [--html-output PATH]

Options:
  --html              Also write trivy-report.html in the project root.
  --html-output PATH  Also write an HTML report to PATH.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --html)
      html_report="trivy-report.html"
      ;;
    --html-output)
      if [[ $# -lt 2 ]]; then
        echo "--html-output requires a path." >&2
        exit 2
      fi
      html_report="$2"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required to run the Trivy vulnerability scan." >&2
  exit 1
fi

mkdir -p "$cache_dir"

run_trivy() {
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    --volume "$PWD:/workspace:ro" \
    --volume "$cache_dir:/trivy-cache" \
    --workdir /workspace \
    "$trivy_image" \
    filesystem \
    --cache-dir /trivy-cache \
    --scanners vuln \
    --severity HIGH,CRITICAL \
    --no-progress \
    "$@" \
    .
}

set +e
run_trivy --exit-code 1
scan_status=$?
set -e

if [[ -n "$html_report" ]]; then
  run_trivy \
    --exit-code 0 \
    --format template \
    --template '@/contrib/html.tpl' > "$html_report"
  echo "HTML report written to $html_report"
fi

exit "$scan_status"
