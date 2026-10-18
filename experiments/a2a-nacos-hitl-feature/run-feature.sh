#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd "$(dirname "$0")" && pwd)"
LAB_ROOT="${LAB_ROOT:-/Users/cckowin/test/spring-ai-alibaba-lab}"
PROFILE="${1:?Usage: run-feature.sh remote|client}"
[[ "$PROFILE" == remote || "$PROFILE" == client ]] || { echo 'Expected remote or client' >&2; exit 1; }
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
mkdir -p "$LAB_ROOT/evidence/feature-ui/$PROFILE" "$LAB_ROOT/logs/feature-ui-nacos-$PROFILE" "$LAB_ROOT/nacos-cache-feature/$PROFILE"
# User supplies Nacos client credentials using NACOS_USERNAME / NACOS_PASSWORD,
# or an external properties file in LAB_CONFIG. Never store them in this repository.
ARGS=("--spring.profiles.active=$PROFILE")
if [[ -n "${LAB_CONFIG:-}" ]]; then ARGS+=("--spring.config.additional-location=file:$LAB_CONFIG"); fi
exec "$JAVA_HOME/bin/java" \
 -Devidence.dir="$LAB_ROOT/evidence/feature-ui/$PROFILE" \
 -Dbusiness.url="${BUSINESS_URL:-http://127.0.0.1:18080}" \
 -DJM.LOG.PATH="$LAB_ROOT/logs/feature-ui-nacos-$PROFILE" -DJM.SNAPSHOT.PATH="$LAB_ROOT/nacos-cache-feature/$PROFILE" \
 -jar "$APP_ROOT/target/a2a-nacos-hitl-feature-1.0.0.jar" "${ARGS[@]}"
