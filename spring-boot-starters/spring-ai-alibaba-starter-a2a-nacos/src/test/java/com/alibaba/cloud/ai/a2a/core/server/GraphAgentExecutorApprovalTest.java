/*
 * Copyright 2025-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.a2a.core.server;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata.ToolFeedback;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata.ToolFeedback.FeedbackResult;
import com.alibaba.cloud.ai.graph.agent.BaseAgent;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalDecision;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalRequest;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ToolDecision;
import com.alibaba.cloud.ai.graph.checkpoint.Checkpoint;
import com.alibaba.cloud.ai.graph.state.StateSnapshot;
import io.a2a.server.agentexecution.RequestContext;
import io.a2a.server.events.EventQueue;
import io.a2a.spec.DataPart;
import io.a2a.spec.Event;
import io.a2a.spec.InvalidParamsError;
import io.a2a.spec.Message;
import io.a2a.spec.MessageSendParams;
import io.a2a.spec.Part;
import io.a2a.spec.TaskState;
import io.a2a.spec.TaskNotCancelableError;
import io.a2a.spec.TaskArtifactUpdateEvent;
import io.a2a.spec.TaskStatusUpdateEvent;
import io.a2a.spec.TextPart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphAgentExecutorApprovalTest {

	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-15T10:00:00Z"), ZoneOffset.UTC);

	private BaseAgent agent;

	private GraphAgentExecutor executor;

	private InterruptionMetadata interruption;

	private final AtomicInteger effects = new AtomicInteger();

	private final AtomicInteger resumes = new AtomicInteger();

	@BeforeEach
	void setUp() throws Exception {
		agent = mock(BaseAgent.class);
		when(agent.getOutputKey()).thenReturn("result");
		interruption = InterruptionMetadata.builder("HITL", new OverAllState(Map.of("private-state", "not-on-wire")))
				.addToolFeedback(new ToolFeedback("tool-1", "write", "{\"amount\":10}", null, "Approve write"))
				.build();
		when(agent.stream(anyString(), any(RunnableConfig.class))).thenReturn(Flux.just(interruption));
		when(agent.getCurrentState(any(RunnableConfig.class))).thenReturn(snapshot("checkpoint-1"));
		when(agent.stream(anyMap(), any(RunnableConfig.class))).thenAnswer(call -> {
			assertThat((Map<String, Object>) call.getArgument(0)).isEmpty();
			RunnableConfig config = call.getArgument(1);
			assertThat(config.threadId()).contains("original-thread");
			assertThat(config.checkPointId()).contains("checkpoint-1");
			InterruptionMetadata feedback = (InterruptionMetadata) config.metadata(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY)
					.orElseThrow();
			assertThat(feedback.node()).isEqualTo(interruption.node());
			assertThat(feedback.state()).isSameAs(interruption.state());
			assertThat(feedback.toolFeedbacks().get(0).getArguments()).isEqualTo("{\"amount\":10}");
			resumes.incrementAndGet();
			if (feedback.toolFeedbacks().get(0).getResult() == FeedbackResult.APPROVED) {
				effects.incrementAndGet();
			}
			return Flux.just(done());
		});
		executor = new GraphAgentExecutor(agent, CLOCK, Duration.ofMinutes(15), Duration.ofSeconds(1));
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void pauseIsFinalInteractionAndNeverCompleted(boolean streaming) {
		Capture capture = new Capture();
		assertTimeoutPreemptively(Duration.ofSeconds(1), () -> executor.execute(initial(streaming), capture.queue));
		TaskStatusUpdateEvent status = capture.last();
		assertThat(status.getStatus().state()).isEqualTo(TaskState.INPUT_REQUIRED);
		assertThat(status.isFinal()).isTrue();
		assertThat(status.getTaskId()).isEqualTo("task-1");
		assertThat(status.getContextId()).isEqualTo("context-1");
		assertThat(capture.statuses()).noneMatch(event -> event.getStatus().state() == TaskState.COMPLETED);
		Map<String, Object> envelope = ((DataPart) status.getStatus().message().getParts().get(0)).getData();
		assertThat(envelope.toString()).doesNotContain("private-state", "not-on-wire", "checkpoint-1", "original-thread");
		assertThat(effects).hasValue(0);
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void approvalResumesExactCheckpointOnceIncludingConcurrentRetry(boolean streaming) {
		ApprovalRequest request = pause(streaming);
		ApprovalDecision decision = decision(request, "APPROVED");
		Capture first = new Capture();
		Capture second = new Capture();
		CompletableFuture.allOf(
				CompletableFuture.runAsync(() -> executor.execute(resume(decision, metadata(streaming)), first.queue)),
				CompletableFuture.runAsync(() -> executor.execute(resume(decision, metadata(streaming)), second.queue))).join();
		assertThat(first.last().getStatus().state()).isEqualTo(TaskState.COMPLETED);
		assertThat(second.last().getStatus().state()).isEqualTo(TaskState.COMPLETED);
		assertThat(effects).hasValue(1);
		assertThat(resumes).hasValue(1);
		assertThat(first.events).anyMatch(TaskArtifactUpdateEvent.class::isInstance);
		assertThat(second.events).anyMatch(TaskArtifactUpdateEvent.class::isInstance);
		assertThatThrownBy(() -> executor.execute(resume(decision(request, "REJECTED"), metadata(streaming)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThat(effects).hasValue(1);
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void rejectionReachesTypedHookWithoutBusinessEffect(boolean streaming) {
		ApprovalRequest request = pause(streaming);
		Capture capture = new Capture();
		executor.execute(resume(decision(request, "REJECTED"), metadata(streaming)), capture.queue);
		assertThat(capture.last().getStatus().state()).isEqualTo(TaskState.COMPLETED);
		assertThat(resumes).hasValue(1);
		assertThat(effects).hasValue(0);
	}

	@ParameterizedTest
	@ValueSource(strings = { "EDITED", "UNKNOWN", "approved", "" })
	void unknownOrUnsupportedResultsFailClosedAndValidDecisionStillWorks(String result) {
		ApprovalRequest request = pause(false);
		assertInvalidThenValid(request, decision(request, result), metadata(false));
	}

	@Test
	void missingPartialExtraAndDuplicateDecisionsFailClosed() {
		ApprovalRequest request = pause(false);
		ToolDecision valid = decision(request, "APPROVED").decisions().get(0);
		for (List<ToolDecision> values : List.of(List.<ToolDecision>of(), List.of(valid, valid),
				List.of(new ToolDecision("tool-1", "wrong-name", valid.argumentsSha256(), "APPROVED")))) {
			assertThatThrownBy(() -> executor.execute(resume(new ApprovalDecision(ApprovalProtocol.PROFILE,
					request.requestId(), request.taskId(), request.contextId(), values), metadata(false)), new Capture().queue))
					.isInstanceOf(InvalidParamsError.class);
		}
		assertThatThrownBy(() -> executor.execute(resume(new ApprovalDecision(ApprovalProtocol.PROFILE,
				request.requestId(), request.taskId(), request.contextId(), null), metadata(false)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertInvalidThenValid(request, new ApprovalDecision(ApprovalProtocol.PROFILE, request.requestId(),
				request.taskId(), request.contextId(), List.of(new ToolDecision("tool-1", "write", "tampered", "APPROVED"))),
				metadata(false));
	}

	@Test
	void nullDecisionResultFailsClosed() {
		ApprovalRequest request = pause(false);
		assertInvalidThenValid(request, decision(request, null), metadata(false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "userId", "threadId", "approvalProfile" })
	void wrongPeerThreadOrProfileCannotConsumeApproval(String key) {
		ApprovalRequest request = pause(false);
		Map<String, Object> wrong = new HashMap<>(metadata(false));
		wrong.put(key, "wrong");
		assertInvalidThenValid(request, decision(request, "APPROVED"), wrong);
	}

	@Test
	void wrongTaskContextRequestIdAndProfileAreRejected() {
		ApprovalRequest request = pause(false);
		ApprovalDecision valid = decision(request, "APPROVED");
		for (ApprovalDecision wrong : List.of(
				new ApprovalDecision("wrong", request.requestId(), request.taskId(), request.contextId(), valid.decisions()),
				new ApprovalDecision(ApprovalProtocol.PROFILE, "wrong", request.taskId(), request.contextId(), valid.decisions()),
				new ApprovalDecision(ApprovalProtocol.PROFILE, request.requestId(), "wrong", request.contextId(), valid.decisions()),
				new ApprovalDecision(ApprovalProtocol.PROFILE, request.requestId(), request.taskId(), "wrong", valid.decisions()))) {
			assertThatThrownBy(() -> executor.execute(resume(wrong, metadata(false)), new Capture().queue))
					.isInstanceOf(InvalidParamsError.class);
		}
		assertThat(effects).hasValue(0);
		executor.execute(resume(valid, metadata(false)), new Capture().queue);
		assertThat(effects).hasValue(1);
	}

	@Test
	void plainTextCannotResumePendingApproval() {
		pause(false);
		assertThatThrownBy(() -> executor.execute(initial(false), new Capture().queue)).isInstanceOf(InvalidParamsError.class);
		assertThat(effects).hasValue(0);
	}

	@Test
	void changedCheckpointAndLostStateDoNotResume() throws Exception {
		ApprovalRequest request = pause(false);
		when(agent.getCurrentState(any(RunnableConfig.class))).thenReturn(snapshot("changed"));
		assertThatThrownBy(() -> executor.execute(resume(decision(request, "APPROVED"), metadata(false)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		GraphAgentExecutor restarted = new GraphAgentExecutor(agent);
		assertThatThrownBy(() -> restarted.execute(resume(decision(request, "APPROVED"), metadata(false)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThat(resumes).hasValue(0);
	}

	@Test
	void expiredApprovalDoesNotResume() {
		executor = new GraphAgentExecutor(agent, CLOCK, Duration.ZERO, Duration.ofSeconds(1));
		ApprovalRequest request = pause(false);
		assertThatThrownBy(() -> executor.execute(resume(decision(request, "APPROVED"), metadata(false)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThat(effects).hasValue(0);
	}

	@Test
	void reservedWireControlMetadataIsRejected() {
		Map<String, Object> values = new HashMap<>(metadata(false));
		values.put(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY, interruption);
		assertThatThrownBy(() -> executor.execute(context(List.of(new TextPart("write")), values), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThat(effects).hasValue(0);
	}

	@Test
	void legacyReadOnlyCompletesButLegacyPauseNeverClaimsSuccess() throws Exception {
		Capture paused = new Capture();
		executor.execute(context(List.of(new TextPart("write")), Map.of()), paused.queue);
		assertThat(paused.last().getStatus().state()).isEqualTo(TaskState.FAILED);
		when(agent.stream(anyString(), any(RunnableConfig.class))).thenReturn(Flux.just(done()));
		Capture completed = new Capture();
		executor.execute(context(List.of(new TextPart("read")), Map.of()), completed.queue);
		assertThat(completed.last().getStatus().state()).isEqualTo(TaskState.COMPLETED);
	}

	@Test
	void runTimeoutReturnsFailureInsteadOfPollingForever() throws Exception {
		when(agent.stream(anyString(), any(RunnableConfig.class))).thenReturn(Flux.never());
		executor = new GraphAgentExecutor(agent, CLOCK, Duration.ofMinutes(15), Duration.ofMillis(25));
		Capture capture = new Capture();
		assertTimeoutPreemptively(Duration.ofSeconds(1), () -> executor.execute(initial(true), capture.queue));
		assertThat(capture.last().getStatus().state()).isEqualTo(TaskState.FAILED);
	}

	@Test
	void cancelPausedApprovalPreventsResumeAndConsumedTaskCannotBeCanceled() {
		ApprovalRequest request = pause(false);
		Capture canceled = new Capture();
		executor.cancel(initial(false), canceled.queue);
		assertThat(canceled.last().getStatus().state()).isEqualTo(TaskState.CANCELED);
		assertThatThrownBy(() -> executor.execute(resume(decision(request, "APPROVED"), metadata(false)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThat(effects).hasValue(0);
		assertThatThrownBy(() -> executor.cancel(initial(false), new Capture().queue)).isInstanceOf(TaskNotCancelableError.class);
	}

	@Test
	void completedActionIsNotReportedAsCanceled() {
		ApprovalRequest request = pause(false);
		executor.execute(resume(decision(request, "APPROVED"), metadata(false)), new Capture().queue);
		Capture capture = new Capture();
		assertThatThrownBy(() -> executor.cancel(initial(false), capture.queue)).isInstanceOf(TaskNotCancelableError.class);
		assertThat(capture.events).isEmpty();
		assertThat(effects).hasValue(1);
	}

	@Test
	void missingPeerAndMalformedPayloadAreRejectedBeforeGraphRun() {
		Map<String, Object> noPeer = new HashMap<>(metadata(false));
		noPeer.remove("userId");
		assertThatThrownBy(() -> executor.execute(context(List.of(new TextPart("write")), noPeer), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThatThrownBy(() -> executor.execute(context(List.of(new DataPart(Map.of("type", ApprovalProtocol.DECISION_TYPE,
				"payload", Map.of("unexpected", "value")))), metadata(false)), new Capture().queue))
				.isInstanceOf(InvalidParamsError.class);
		assertThat(resumes).hasValue(0);
	}

	@Test
	void argumentDigestSortsNestedObjectsAndPreservesArrayOrderAndPrecision() {
		assertThat(ApprovalProtocol.sha256("{\"b\":2,\"a\":{\"y\":4,\"x\":3}}"))
				.isEqualTo(ApprovalProtocol.sha256("{\"a\":{\"x\":3,\"y\":4},\"b\":2}"));
		assertThat(ApprovalProtocol.sha256("[1,2]")).isNotEqualTo(ApprovalProtocol.sha256("[2,1]"));
		assertThat(ApprovalProtocol.sha256("{\"amount\":0.123456789012345678901}"))
				.isNotEqualTo(ApprovalProtocol.sha256("{\"amount\":0.123456789012345678902}"));
		assertThatThrownBy(() -> ApprovalProtocol.sha256("{\"a\":1,\"a\":2}"))
				.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> ApprovalProtocol.sha256("{} {}"))
				.isInstanceOf(IllegalArgumentException.class);
	}

	private void assertInvalidThenValid(ApprovalRequest request, ApprovalDecision invalid, Map<String, Object> metadata) {
		Capture capture = new Capture();
		assertThatThrownBy(() -> executor.execute(resume(invalid, metadata), capture.queue)).isInstanceOf(InvalidParamsError.class);
		assertThat(capture.events).isEmpty();
		assertThat(effects).hasValue(0);
		executor.execute(resume(decision(request, "APPROVED"), metadata(false)), new Capture().queue);
		assertThat(effects).hasValue(1);
	}

	private ApprovalRequest pause(boolean streaming) {
		Capture capture = new Capture();
		executor.execute(initial(streaming), capture.queue);
		assertThat(capture.last().getStatus().state()).isEqualTo(TaskState.INPUT_REQUIRED);
		DataPart data = (DataPart) capture.last().getStatus().message().getParts().get(0);
		return ApprovalProtocol.parse(data.getData().get("payload"), ApprovalRequest.class);
	}

	private ApprovalDecision decision(ApprovalRequest request, String result) {
		return new ApprovalDecision(ApprovalProtocol.PROFILE, request.requestId(), request.taskId(), request.contextId(),
				request.tools().stream().map(tool -> new ToolDecision(tool.id(), tool.name(), tool.argumentsSha256(), result)).toList());
	}

	private RequestContext initial(boolean streaming) {
		return context(List.of(new TextPart("write")), metadata(streaming));
	}

	private RequestContext resume(ApprovalDecision decision, Map<String, Object> metadata) {
		return context(List.of(new DataPart(ApprovalProtocol.envelope(ApprovalProtocol.DECISION_TYPE, decision))), metadata);
	}

	private RequestContext context(List<Part<?>> parts, Map<String, Object> metadata) {
		Message message = new Message.Builder().role(Message.Role.USER).messageId(UUID.randomUUID().toString())
				.taskId("task-1").contextId("context-1").parts(parts).build();
		return new RequestContext(new MessageSendParams(message, null, metadata), "task-1", "context-1", null, null);
	}

	private Map<String, Object> metadata(boolean streaming) {
		return Map.of("threadId", "original-thread", "userId", "trusted-test-peer", "isStreaming", streaming,
				ApprovalProtocol.PROFILE_METADATA_KEY, ApprovalProtocol.PROFILE);
	}

	private StateSnapshot snapshot(String checkpointId) {
		Checkpoint checkpoint = Checkpoint.builder().id(checkpointId).nodeId("model").nextNodeId("HITL")
				.state(Map.of("private-state", "not-on-wire")).build();
		return StateSnapshot.of(Map.of(), checkpoint, RunnableConfig.builder().threadId("original-thread").build(), OverAllState::new);
	}

	private NodeOutput done() {
		return NodeOutput.of("__END__", "agent", new OverAllState(Map.of("result", "done")), null);
	}

	private static class Capture {

		final List<Event> events = new ArrayList<>();

		final EventQueue queue = mock(EventQueue.class);

		Capture() {
			doAnswer(call -> {
				events.add(call.getArgument(0));
				return null;
			}).when(queue).enqueueEvent(any(Event.class));
		}

		List<TaskStatusUpdateEvent> statuses() {
			return events.stream().filter(TaskStatusUpdateEvent.class::isInstance).map(TaskStatusUpdateEvent.class::cast).toList();
		}

		TaskStatusUpdateEvent last() {
			List<TaskStatusUpdateEvent> values = statuses();
			return values.get(values.size() - 1);
		}

	}

}
