#!/usr/bin/env bash
# scripts/guard.sh
set -euo pipefail

# Absolute dir of *this* guard file  (…/your-project/scripts)
_GUARD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# One level up  (…/your-project)
_PROJECT_ROOT="$(dirname "$_GUARD_DIR")"

if [[ "$PWD" != "$_PROJECT_ROOT" ]]; then
  echo "✖  Please run scripts from the project root ($_PROJECT_ROOT)."
  echo "   For example:  cd $_PROJECT_ROOT && ./scripts/$(basename "${BASH_SOURCE[1]}")"
  exit 1
fi
