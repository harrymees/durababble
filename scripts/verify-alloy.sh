#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALLOY_VERSION="${ALLOY_VERSION:-6.2.0}"
ALLOY_OUTPUT_DIR="${ALLOY_OUTPUT_DIR:-$ROOT/tmp/alloy-output}"
ALLOY_JAR="${ALLOY_JAR:-$ROOT/tmp/alloy/org.alloytools.alloy.dist-$ALLOY_VERSION.jar}"
ALLOY_URL="${ALLOY_URL:-https://repo1.maven.org/maven2/org/alloytools/org.alloytools.alloy.dist/$ALLOY_VERSION/org.alloytools.alloy.dist-$ALLOY_VERSION.jar}"

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
    curl --fail --location --show-error --silent "$ALLOY_URL" --output "$ALLOY_JAR"
  fi

  if command -v java >/dev/null 2>&1; then
    alloy=(java -jar "$ALLOY_JAR")
  elif command -v mise >/dev/null 2>&1; then
    alloy=(mise exec java@temurin-21 -- java -jar "$ALLOY_JAR")
  else
    echo "Missing Alloy runtime: install alloy6 or provide java for $ALLOY_JAR" >&2
    exit 1
  fi
fi

for model in "${models[@]}"; do
  echo "Verifying $model"
  "${alloy[@]}" exec -f -s glucose -o "$ALLOY_OUTPUT_DIR" "$model"
done
