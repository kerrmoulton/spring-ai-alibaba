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
