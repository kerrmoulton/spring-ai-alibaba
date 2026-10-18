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

Without restarting or rebuilding the running services, the dedicated experiment page was tested
through native Chrome UI clicks on 2026-10-18. Runtime source commit remains
`402886b2d6299150aed0c1c5206a88606a937e80`; later documentation commits change no runtime code.

- `browser_feature_approve_001`, thread `e8123cb0-bdf1-42d6-a3f3-1e7af9cae3ae`:
  actual remote approval card, click Approve then a real browser `dblclick()` on
  Submit All Feedback. Before: reads=1/writes=0; afterward: reads=1/writes=1.
- `browser_feature_reject_001`, thread `9166b225-262b-4a9a-914a-29a66bfa43a2`:
  actual Reject then Submit, visible rejection result, reads=1/writes=0.
- Submit was disabled before a decision, enabled after choosing, and displayed disabled
  Submitted afterward (`isEnabled() == false`). The actual double-click did not add a second write.
- The native card still says Processing/Waiting while the final result is visible below it.
  This display limitation is documented, not hidden.

Actual screenshots and raw UI-side supporting audit/status are in
`/Users/cckowin/Documents/Codex/2026-10-18/task/feature-browser-evidence`, with a self-contained
HTML acceptance report and Library delivery metadata. Original baseline browser observations
were performed by root and have audit/status JSON; no baseline screenshot file was saved.
No original screenshot was reconstructed, and baseline was not rerun for these UI checks.

`/repro/status.frameworkCommit` retains a baseline label and does not identify the actual feature
runtime revision. Use the executable/nested-jar hashes in `lab/evidence/feature/build-provenance.json`.

## Shutdown

Do not stop unrelated services. For the currently verified run, client PID=26308 and remote
PID=24697; after confirming those PIDs still belong to this experiment:

```bash
kill 26308 24697
```

To stop only this experiment's Nacos through its guarded launcher:

```bash
python3 /Users/cckowin/test/spring-ai-alibaba-lab/nacos-lab.py stop
```

Start Nacos with the same launcher using `start`; start remote profile, wait for its Nacos
registration success, then start client profile. Existing services were left running for review.
