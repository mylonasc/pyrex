#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/runpath_guard.sh"

set -euo pipefail

trivy_image="${TRIVY_IMAGE:-aquasec/trivy:0.74.0}"
uv_image="${UV_IMAGE:-ghcr.io/astral-sh/uv:0.8.22-python3.13-bookworm-slim}"
cache_dir="${TRIVY_CACHE_DIR:-$HOME/.cache/trivy}"
html_report=""
dependency_dir=""

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

dependency_dir="$(mktemp -d)"
trap 'rm -rf "$dependency_dir"' EXIT
mkdir -p "$dependency_dir/package" "$dependency_dir/build" "$dependency_dir/docs"

echo "Resolving Python dependencies from pyproject.toml"
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --env UV_NO_CACHE=1 \
  --volume "$PWD:/workspace:ro" \
  --volume "$dependency_dir:/output" \
  --workdir /workspace \
  "$uv_image" \
  /usr/local/bin/uv --quiet pip compile \
  pyproject.toml \
  --all-extras \
  --universal \
  --no-annotate \
  --no-header \
  --output-file /output/package/requirements.txt

docker run --rm \
  --user "$(id -u):$(id -g)" \
  --volume "$PWD:/workspace:ro" \
  "$uv_image" \
  python -c 'import pathlib, tomllib; data = tomllib.loads(pathlib.Path("/workspace/pyproject.toml").read_text()); print("\n".join(data["build-system"]["requires"]))' \
  > "$dependency_dir/build/requirements.in"

docker run --rm \
  --user "$(id -u):$(id -g)" \
  --env UV_NO_CACHE=1 \
  --volume "$dependency_dir:/output" \
  "$uv_image" \
  /usr/local/bin/uv --quiet pip compile \
  /output/build/requirements.in \
  --universal \
  --no-annotate \
  --no-header \
  --output-file /output/build/requirements.txt

if [[ -f docs/requirements.txt ]]; then
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    --env UV_NO_CACHE=1 \
    --volume "$PWD:/workspace:ro" \
    --volume "$dependency_dir:/output" \
    "$uv_image" \
    /usr/local/bin/uv --quiet pip compile \
    /workspace/docs/requirements.txt \
    --universal \
    --no-annotate \
    --no-header \
    --output-file /output/docs/requirements.txt
fi

run_trivy() {
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    --volume "$PWD:/scan/project:ro" \
    --volume "$dependency_dir:/scan/python-dependencies:ro" \
    --volume "$cache_dir:/trivy-cache" \
    --workdir /scan \
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
