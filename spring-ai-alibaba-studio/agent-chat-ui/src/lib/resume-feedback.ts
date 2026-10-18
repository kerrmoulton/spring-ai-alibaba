/** Tracks the actual result of a feedback resume stream, rather than mere submission. */
export type FeedbackResumeOutcome = "completed" | "approval-required";

type ResumeResponse = {
  error?: unknown;
  errorMessage?: string;
  chunk?: string | null;
  message?: { messageType: string; content?: string } | null;
};

export class FeedbackResumeTracker {
  private outcome: FeedbackResumeOutcome | null = null;

  accept(response: ResumeResponse): void {
    if (response.error) {
      throw new Error(response.errorMessage || "Feedback execution failed");
    }
    if (response.message?.messageType === "tool-confirm") {
      this.outcome = "approval-required";
    } else if (this.outcome !== "approval-required" && (response.chunk ||
      (response.message?.messageType === "assistant" && response.message.content))) {
      this.outcome = "completed";
    }
  }

  finish(): FeedbackResumeOutcome {
    if (this.outcome === null) {
      throw new Error("Feedback stream ended without a result or another approval request");
    }
    return this.outcome;
  }
}
