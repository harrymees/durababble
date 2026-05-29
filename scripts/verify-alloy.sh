#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_ALLOY_VERSION="6.2.0"
DEFAULT_ALLOY_SHA256="6037cbeee0e8423c1c468447ed10f5fcf2f2743a2ffc39cb1c81f2905c0fdb9d"
ALLOY_VERSION="${ALLOY_VERSION:-$DEFAULT_ALLOY_VERSION}"
ALLOY_OUTPUT_DIR="${ALLOY_OUTPUT_DIR:-$ROOT/tmp/alloy-output}"
ALLOY_JAR="${ALLOY_JAR:-$ROOT/tmp/alloy/org.alloytools.alloy.dist-$ALLOY_VERSION.jar}"
ALLOY_URL="${ALLOY_URL:-https://repo1.maven.org/maven2/org/alloytools/org.alloytools.alloy.dist/$ALLOY_VERSION/org.alloytools.alloy.dist-$ALLOY_VERSION.jar}"
ALLOY_SHA256="${ALLOY_SHA256:-}"
ALLOY_SOLVER="${ALLOY_SOLVER:-glucose}"
ALLOY_OUTPUT_TYPE="${ALLOY_OUTPUT_TYPE:-none}"
ALLOY_COMMAND="${ALLOY_COMMAND:-}"

if [[ -z "$ALLOY_SHA256" && "$ALLOY_VERSION" == "$DEFAULT_ALLOY_VERSION" ]]; then
  ALLOY_SHA256="$DEFAULT_ALLOY_SHA256"
fi

checksum_command=()
if command -v sha256sum >/dev/null 2>&1; then
  checksum_command=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  checksum_command=(shasum -a 256)
else
  echo "Missing SHA-256 checksum tool: install sha256sum or shasum" >&2
  exit 1
fi

checksum_for() {
  "${checksum_command[@]}" "$1" | awk '{print tolower($1)}'
}

verify_alloy_jar() {
  local jar="$1"

  if [[ -z "$ALLOY_SHA256" ]]; then
    echo "Missing ALLOY_SHA256 for Alloy $ALLOY_VERSION; refusing to use unverified $jar" >&2
    exit 1
  fi

  if [[ ! "$ALLOY_SHA256" =~ ^[0-9A-Fa-f]{64}$ ]]; then
    echo "Invalid ALLOY_SHA256 for Alloy $ALLOY_VERSION: expected 64 hex characters" >&2
    exit 1
  fi

  local expected
  expected="$(printf '%s' "$ALLOY_SHA256" | tr '[:upper:]' '[:lower:]')"
  local actual
  actual="$(checksum_for "$jar")"
  if [[ "$actual" != "$expected" ]]; then
    echo "Alloy jar checksum mismatch for $jar" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    return 1
  fi
}

cd "$ROOT"

models=()
while IFS= read -r -d '' file; do
  models+=("$file")
done < <(find formal -name '*.als' -print0 | sort -z)

if [[ ${#models[@]} -eq 0 ]]; then
  echo "No Alloy models found under formal/" >&2
  exit 1
fi

mkdir -p "$ALLOY_OUTPUT_DIR"

if command -v alloy6 >/dev/null 2>&1; then
  alloy=(alloy6)
else
  if [[ ! -f "$ALLOY_JAR" ]]; then
    mkdir -p "$(dirname "$ALLOY_JAR")"
    tmp_jar="$(mktemp "$(dirname "$ALLOY_JAR")/.alloy-download.XXXXXX")"
    trap 'rm -f "$tmp_jar"' EXIT
    curl --fail --location --show-error --silent "$ALLOY_URL" --output "$tmp_jar"
    verify_alloy_jar "$tmp_jar"
    mv "$tmp_jar" "$ALLOY_JAR"
    trap - EXIT
  else
    verify_alloy_jar "$ALLOY_JAR" || {
      rm -f "$ALLOY_JAR"
      echo "Removed cached Alloy jar with invalid checksum; rerun to download a verified copy" >&2
      exit 1
    }
  fi

  if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
    alloy=(java -jar "$ALLOY_JAR")
  elif command -v mise >/dev/null 2>&1; then
    alloy=(mise exec -- java -jar "$ALLOY_JAR")
  else
    echo "Missing Alloy runtime: install alloy6 or provide java for $ALLOY_JAR" >&2
    exit 1
  fi
fi

for model in "${models[@]}"; do
  echo "Verifying $model"
  if [[ -n "$ALLOY_COMMAND" ]]; then
    "${alloy[@]}" exec -f -s "$ALLOY_SOLVER" -t "$ALLOY_OUTPUT_TYPE" -c "$ALLOY_COMMAND" -o "$ALLOY_OUTPUT_DIR" "$model"
  else
    "${alloy[@]}" exec -f -s "$ALLOY_SOLVER" -t "$ALLOY_OUTPUT_TYPE" -o "$ALLOY_OUTPUT_DIR" "$model"
  fi
  if grep -q '"expects":-1' "$ALLOY_OUTPUT_DIR/receipt.json"; then
    echo "All Alloy commands in $model must declare an expect result" >&2
    exit 1
  fi
done
