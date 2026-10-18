import assert from "node:assert/strict";
import test from "node:test";
import { FeedbackResumeTracker } from "./resume-feedback.ts";

test("approval and rejection require an actual response before completion", () => {
  for (const content of ["Order created", "Rejected. No order created."]) {
    const tracker = new FeedbackResumeTracker();
    assert.throws(() => tracker.finish(), /without a result/);
    tracker.accept({ message: { messageType: "assistant", content } });
    assert.equal(tracker.finish(), "completed");
  }
});

test("a subsequent real approval is not reported as completed", () => {
  const tracker = new FeedbackResumeTracker();
  tracker.accept({ chunk: "Preparing another tool" });
  tracker.accept({ message: { messageType: "tool-confirm", content: "Approve tool" } });
  assert.equal(tracker.finish(), "approval-required");
});

test("SSE error after a partial result must reject the resume", () => {
  const tracker = new FeedbackResumeTracker();
  tracker.accept({ chunk: "Partial result" });
  assert.throws(() => tracker.accept({ error: true, errorMessage: "Execution failed" }), /Execution failed/);
});

test("empty, heartbeat and tool-only streams do not claim success", () => {
  const tracker = new FeedbackResumeTracker();
  tracker.accept({});
  tracker.accept({ message: { messageType: "tool", content: "tool output" } });
  assert.throws(() => tracker.finish(), /without a result/);
});

test("a pending approval wins over later assistant chunks in the same stream", () => {
  const tracker = new FeedbackResumeTracker();
  tracker.accept({ message: { messageType: "tool-confirm", content: "Approve next tool" } });
  tracker.accept({ chunk: "The next approval is pending" });
  assert.equal(tracker.finish(), "approval-required");
});
