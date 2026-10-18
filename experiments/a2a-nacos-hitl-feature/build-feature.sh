#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$APP_ROOT/../.." && pwd)"
LAB_ROOT="${LAB_ROOT:-/Users/cckowin/test/spring-ai-alibaba-lab}"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
MAVEN="${MAVEN:-/opt/homebrew/bin/mvn}"
[[ "$(git -C "$REPO_ROOT" branch --show-current)" == feature/a2a-hitl-bridge ]] || exit 1
[[ -f "$LAB_ROOT/archives/baseline-20261018/manifest.json" ]] || exit 1
ARGS=(-s "$APP_ROOT/maven-settings.xml" -B -ntp -Dmaven.repo.local="$LAB_ROOT/m2-feature")
"$MAVEN" "${ARGS[@]}" -f "$REPO_ROOT/pom.xml" \
 -pl spring-ai-alibaba-studio,spring-boot-starters/spring-ai-alibaba-starter-a2a-nacos -am install \
 -Dmaven.test.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dspring-javaformat.skip=true \
 -Dcheckstyle.skip=true -Dspotless.skip=true -Dgpg.skip=true -Dspringdoc.skip=true \
 > "$LAB_ROOT/logs/framework-feature-build.log" 2>&1
"$MAVEN" "${ARGS[@]}" -f "$APP_ROOT/pom.xml" package -DskipTests > "$LAB_ROOT/logs/app-feature-build.log" 2>&1
