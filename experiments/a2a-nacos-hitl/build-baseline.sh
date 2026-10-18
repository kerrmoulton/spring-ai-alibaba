#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$APP_ROOT/../.." && pwd)"
LAB_ROOT="${LAB_ROOT:-/Users/cckowin/test/spring-ai-alibaba-lab}"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
MAVEN="${MAVEN:-/opt/homebrew/bin/mvn}"
mkdir -p "$LAB_ROOT/m2" "$LAB_ROOT/logs"
[[ "$(git -C "$REPO_ROOT" rev-parse HEAD)" == 17e26ff6fe951d605771c1845229ca3edb043634 ]] || { echo 'Unexpected baseline HEAD' >&2; exit 1; }
# App files are untracked additions; any tracked framework modification is prohibited here.
git -C "$REPO_ROOT" diff --quiet && git -C "$REPO_ROOT" diff --cached --quiet || { echo 'Tracked framework changes detected' >&2; exit 1; }
"$MAVEN" -s "$APP_ROOT/maven-settings.xml" -B -ntp -Dmaven.repo.local="$LAB_ROOT/m2" -f "$REPO_ROOT/pom.xml" \
 -pl spring-ai-alibaba-studio,spring-boot-starters/spring-ai-alibaba-starter-a2a-nacos -am install \
 -Dmaven.test.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dspring-javaformat.skip=true \
 -Dspotless.skip=true -Dgpg.skip=true -Dspringdoc.skip=true > "$LAB_ROOT/logs/framework-baseline-build.log" 2>&1
"$MAVEN" -s "$APP_ROOT/maven-settings.xml" -B -ntp -Dmaven.repo.local="$LAB_ROOT/m2" -f "$APP_ROOT/pom.xml" package -DskipTests \
 > "$LAB_ROOT/logs/app-baseline-build.log" 2>&1
