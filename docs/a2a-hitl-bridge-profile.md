# Experimental A2A approval continuation profile

This opt-in implementation targets `spring-ai-alibaba-hitl/0.2.5-v1` with the project's A2A Java SDK `0.2.5.Beta2`. It is not an A2A 1.0 implementation or a production authorization service.

Enable it on a remote client with `A2aRemoteAgent.builder().approvalBridge(true)`. The remote agent still needs a real `HumanInTheLoopHook` and a checkpointer. The parent graph also needs a checkpointer. Use the patched client, server starter and Studio together. Legacy clients do not silently receive approval rights.

## Contract

1. The server sees the original graph `InterruptionMetadata` before reporting completion. It retains the exact checkpoint and tool proposals, and emits an `input-required` task status containing a `DataPart` with type `saa.approval.request`.
2. The request includes profile, random request ID, original task/context IDs, expiry and tool ID/name/arguments/canonical-JSON SHA-256. The client returns a real parent graph interruption rather than a successful text result. Correlation is retained before the graph consumes that control value.
3. Studio requires an explicit, structurally valid decision. The client accepts exactly the pending IDs, names and normalized arguments and only `APPROVED` or `REJECTED`. Remote `EDITED` is deliberately unsupported. UI tool IDs are prefixed with the request ID to reject stale cards even when tool IDs are reused.
4. A continuation sends the typed `saa.approval.decision`, same task/context and a new message ID. It does not resend the original instruction. The server checks its authoritative pending entry, user label, checkpoint and proposal digest, then resumes that exact checkpoint with original arguments and typed human feedback.
5. An atomic process-local consumed receipt prevents the same approved tool from being executed again. Conflicting replay fails; identical replay reuses the settled outcome. Failure after consumption never automatically reruns a possibly completed business action.
6. Both `message/stream` and `message/send` support the profile. Errors are errors, not assistant success text. HTTP SSE forwards completion/error and terminates on a JSON-RPC error. Unknown/unstructured approval states and lost correlation fail closed.

## Deliberate limits

- One parent graph with one remote child, one pending invocation per parent/node. Use globally unique parent thread IDs within a remote-agent instance. Sharing that instance across applications with identical thread IDs is unsupported; this does not provide multi-tenant graph namespaces.
- State and replay receipts are in memory. Restart, another process or missing checkpoints cannot safely resume an old approval. There is no crash-safe exactly-once claim. Production effects require durable, transactional business idempotency and durable approval state.
- `userId` is a trusted-peer binding label, not authenticated approver identity. The demo accepts it from request bodies. A verified principal and authorization boundary must be supplied before production use; do not expose the demo publicly.
- Approval expiry is 15 minutes. Pending/receipt capacity is bounded at 10,000; receipts are not silently evicted. Reaching the bound fails closed.
- Parallel/multiple-parent/distributed execution, multi-instance restart recovery, proposal editing and Nacos refresh are not validated by this experiment. Automatic registration and name-based discovery through local Nacos 3.1.2 are validated by the accompanying a2a-nacos-hitl-feature experiment.
- The client buffers each HTTP interaction before forwarding graph events. This implementation does not promise token-by-token latency.
- Backend verification uses the actual Studio `/run_sse` and `/resume_sse` endpoints and parsed `tool-confirm` DTOs. Browser visual rendering and clicking remain separately unverified.

The local integration harness and raw HTTP/SSE/business-side-effect evidence accompany the feature deliverable. Focused tests are not the entire framework test suite.
