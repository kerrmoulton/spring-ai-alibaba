# Nacos A2A approval bridge feature experiment

Branch: `feature/a2a-hitl-bridge`, based on `17e26ff6fe951d605771c1845229ca3edb043634`.
The original delivered framework-feature.patch is applied to the framework sources.
Application changes from the validated baseline: `approvalBridge(true)` plus passive delegation
of map-input resume and current checkpoint access, required by GraphAgentExecutor.
The common business agent/model/tools and raw order write counter are unchanged.
Nacos still automatically registers server_business and supplies the client AgentCardProvider.

Baseline source/binaries/evidence are preserved under lab/archives/baseline-20261018;
original lab/m2 and lab/evidence/baseline are not used for feature outputs.
Feature Maven repository is lab/m2-feature (copied dependencies, separate files).
Feature evidence is lab/evidence/feature and logs have feature names.

Build: ./build-feature.sh. With the existing loopback Nacos ready, run ./run-feature.sh remote,
wait for automatic registration success, then ./run-feature.sh client. App ports are 18081 and
18080; native Studio is http://127.0.0.1:18080/chatui/ with local_sequential / a2a_sequential.
User-owned Nacos administrator initialization is separate and is never performed by this app.
Optional client credentials can be provided through the same external configuration as baseline.

The bridge is an opt-in process-local profile for SDK 0.2.5.Beta2. Pending approvals and consumed
receipts are in memory, userId is a peer label, and this experiment makes no distributed/crash-safe
exactly-once guarantee. Business API itself does not deduplicate any writes.

## Clone and run on another computer

Requirements: Git, JDK17 (set `JAVA_HOME`, or Java on PATH), Maven3.6+ (`mvn` on PATH,
or set `MAVEN`), Python3. No AI API credentials are needed. Bash scripts run on macOS/Linux;
Windows users can use WSL with JDK/Maven installed inside WSL. Generated Studio UI resources
are committed, so Node/pnpm and the local UI build lock are unnecessary for this runnable clone.
Local evidence, compiled JARs, Nacos distributions, caches and private configuration are not in Git.

```bash
git clone --branch feature/a2a-hitl-bridge https://github.com/kerrmoulton/spring-ai-alibaba.git
cd spring-ai-alibaba
export LAB_ROOT="$(cd .. && pwd)/spring-ai-alibaba-lab"
mkdir -p "$LAB_ROOT/logs"
bash experiments/a2a-nacos-hitl-feature/build-feature.sh
```

This builds the required framework modules and app against an isolated Maven repository;
the historical baseline archive is optional evidence and is not required to build.
Logs are in `$LAB_ROOT/logs`. The script skips framework tests during compilation.

Before starting the app, install and start **Nacos3.1.2** independently using its official
distribution/instructions. The repository does not bundle a Nacos server or authentication
configuration. The original local installation used a separate loopback binding overlay;
that machine-specific launcher is not part of this clone. Keep a fresh test installation
private/loopback or access a Nacos instance you control. Preserve its authentication settings.
Administrator initialization, when prompted, must be completed by the user in Nacos console.
Official setup: https://nacos.io/en/docs/v3.0/quickstart/quick-start/

Default server address is `127.0.0.1:8848`, namespace `public`. To use a different controlled
Nacos address, set `SPRING_AI_ALIBABA_A2A_NACOS_SERVER_ADDR` for **both** application terminals.
Client username/password, only if required by that installation, come from `NACOS_USERNAME`
and `NACOS_PASSWORD`, or `LAB_CONFIG` pointing to an external properties file. Never commit
the file or values. That file can set `spring.ai.alibaba.a2a.nacos.server-addr`, `namespace`,
`username`, and `password`. Do not paste credentials into shell commands or logs. Existing
Nacos client debug/startup logs may include configuration; keep logs private.

Start remote first in one terminal (with the same LAB_ROOT/environment):

```bash
bash experiments/a2a-nacos-hitl-feature/run-feature.sh remote
```

Wait for `Auto register agent server_business ... successfully`, then in a second terminal:

```bash
bash experiments/a2a-nacos-hitl-feature/run-feature.sh client
```

Both apps bind loopback: remote18081, client18080. Keep these ports free. Nacos may run on
another host, but both demo apps must stay together on this computer: the registered card
and business API deliberately use local loopback addresses. This is not a remote deployment guide.

Open `http://127.0.0.1:18080/chatui/index.html?agent=a2a_sequential`, send
`create order case=my_unique_case`, then choose Approve or Reject and Submit. Use a new thread
and unique case for each independent trial. `local_sequential` is the nested local control.

```bash
python3 experiments/a2a-nacos-hitl-feature/test_feature.py
curl -fsS http://127.0.0.1:18080/repro/status
```

The Python harness uses only the standard library, produces six fresh HTTP cases and writes
raw requests/responses plus `feature-test-results.json` under `$LAB_ROOT/evidence/feature-ui/http`.
`allPassed: true` means approve=1/reject=0 and replay/concurrent checks passed. The harness
does not replace actual browser clicks. Stop each foreground app with Ctrl+C; with background
launches, stop only the PIDs you recorded for this JAR. Leave shared Nacos under its owner's control.

The baseline directory is historical experiment source. Its strict baseline build script was
used at upstream commit17e26ff6 before applying this branch; it is not the feature quick start.

## Actual validation on 2026-10-18

Run `python3 test_feature.py` after both profiles are ready. Fresh run
`nacos-feature-9f307d5b` passed all six HTTP integration cases: local and A2A nested
approve/reject, A2A read-only, and two concurrent native Studio approvals.
Remote pause states were submitted / working / input-required, with one genuine graph
InterruptionMetadata and one actual Studio tool-confirm card. Resume preserved request/task/context
IDs, used a fresh message ID and a structured decision instead of replaying the original instruction.
Approved writes=1, rejected writes=0, sequential parent and raw wire replay added no writes;
two concurrent identical Studio submits returned 200 and left writes=1. The business API has
no deduplication. The source callbacks/model/order method are retained from baseline.

Focused unit tests passed: A2aApprovalClientTest 18; Studio controller/DTO tests 57;
server approval/router/SSE tests 41 (116 total, zero failures/errors). These are selected
unit tests and real HTTP integration, not a claim that the full framework suite ran.

Evidence: lab/evidence/feature/http/feature-test-results.json, client/remote-audit.json,
all raw *.request.json / *.response.txt; build and unit logs under lab/logs.
lab/evidence/feature/build-provenance.json records exact executable nested-jar hashes,
source revision and the baseline archive integrity check. Native browser acceptance is performed
separately by the root task.

Root independently confirmed that the user personally initialized and logged into Nacos console,
then visually inspected server_business version 1.0.0, endpoint http://127.0.0.1:18081/a2a,
protocol 0.2.5, JSONRPC and streaming enabled. No administrator password was read or set by agents.

## Actual Chrome UI acceptance

The first feature browser run used source `402886b`, with actual approval/double-click and
rejection evidence archived under `lab/archives/feature-before-ui-20261018`. Its final card
still displayed Processing/Waiting. The authorized UI finish now resolves this display issue.

Current runtime source is `96a0be7844b6ce4cb27a9a7f25e3ecc5a64b1bb0`; the later evidence-documentation
commit changes no runtime code. New native Chrome cases on 2026-10-18:

- `browser_feature_ui_approve_001`, thread `ee85161d-5d9c-47a3-a6b7-36cdb0fa6a41`:
  actual Approve then real locator `dblclick()` on Submit All Feedback; reads1/writes1;
  card shows Approved — Completed, Completed button disabled (`isEnabled() == false`).
- `browser_feature_ui_reject_001`, thread `55423d73-0caf-4c91-81db-1fc222d8c92c`:
  actual Reject then Submit; reads1/writes0; Rejected — Completed, Completed disabled.
- Submit is disabled before a decision and enabled after choosing. A synchronous submission
  ref prevents duplicate UI submissions before React renders. Completion is based on the
  actual resumed assistant response, with errors propagated and new pending approvals retained.

This run passed Studio57 + UI5 + HTTP6; typecheck and static export passed. The 59 unchanged
framework client/server focused tests passed in the prior feature run. The full framework
suite was not run. No business idempotency logic was added.

Screenshots/report and browser audit/status: workspace `feature-browser-evidence`, copied to
`lab/evidence/feature-ui/browser`. New build hashes and runtime PIDs are recorded in
`lab/evidence/feature-ui/build-provenance.json`. The original baseline evidence is preserved;
no original baseline screenshot file was saved or reconstructed.

`/repro/status.frameworkBaseCommit` explicitly identifies upstream base `17e26ff6...`, not the
runtime revision. Use the provenance executable and four matching nested jar hashes for runtime.

### Reproduce the UI build

The upstream tracked pnpm lock is stale relative to its existing manifest. No dependency
manifest or upstream lock upgrade was committed. This experiment's resolved lock is archived
at `lab/evidence/feature-ui/build/pnpm-lock.build.yaml`, with its SHA256 in build-provenance.
To reproduce, copy `spring-ai-alibaba-studio/agent-chat-ui` to a temporary build directory
(exclude node_modules/.next/out), copy that resolved lock as `pnpm-lock.yaml`, and run:

```bash
pnpm install --frozen-lockfile --store-dir /Users/cckowin/test/spring-ai-alibaba-lab/pnpm-store
pnpm exec tsc --noEmit
node --experimental-strip-types --test src/lib/resume-feedback.test.mjs
NEXT_TELEMETRY_DISABLED=1 pnpm run build:static
```

The configured static build exports to the sibling Studio resources directory; preserve that
relative directory structure in the temporary copy. Copy the resulting complete chatui export
back to Studio resources before Maven packaging. Build Studio with isolated m2-feature, then
package this app as documented above. Runtime code commit already contains the generated export.

## Startup and shutdown

Keep existing Nacos running. Start `run-feature.sh remote`, wait for its automatic Nacos
registration success, then start `run-feature.sh client`. Do not restart Nacos for UI changes.
Current remote PID66606 and client PID67634 listen only on 127.0.0.1:18081/18080.
Confirm the PIDs still run this exact experimental JAR before stopping only these processes:

```bash
ps -p 67634,66606 -o pid=,command=
kill 67634 66606
```

Nacos has a separate guarded launcher (only use when explicitly closing the environment):

```bash
python3 /Users/cckowin/test/spring-ai-alibaba-lab/nacos-lab.py status
python3 /Users/cckowin/test/spring-ai-alibaba-lab/nacos-lab.py stop
```

Its console is `http://127.0.0.1:18880/index.html`. User personally completed administrator
initialization and login; real SDK registration/discovery succeeded. Credentials were not
read or set by the agent. Both applications remain running for independent review.
