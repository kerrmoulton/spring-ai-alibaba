/*
 * Copyright 2024-2026 the original author or authors.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata.ToolFeedback;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata.ToolFeedback.FeedbackResult;
import com.alibaba.cloud.ai.graph.agent.Agent;
import com.alibaba.cloud.ai.graph.agent.BaseAgent;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalDecision;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalRequest;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ToolDecision;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ToolProposal;
import com.alibaba.cloud.ai.graph.state.StateSnapshot;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import io.a2a.server.agentexecution.AgentExecutor;
import io.a2a.server.agentexecution.RequestContext;
import io.a2a.server.events.EventQueue;
import io.a2a.server.tasks.TaskUpdater;
import io.a2a.spec.DataPart;
import io.a2a.spec.InvalidParamsError;
import io.a2a.spec.JSONRPCError;
import io.a2a.spec.Message;
import io.a2a.spec.Part;
import io.a2a.spec.Task;
import io.a2a.spec.TaskState;
import io.a2a.spec.TaskNotCancelableError;
import io.a2a.spec.TaskStatus;
import io.a2a.spec.TaskStatusUpdateEvent;
import io.a2a.spec.TextPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;

/**
 * Executes graph runs, including an opt-in SDK 0.2.5.Beta2 approval profile.
 * Approval state is process-local: restart, expiry or checkpoint changes fail closed.
 * The metadata userId is only a binding label supplied by a trusted test peer, NOT
 * authenticated human identity. Production use needs authenticated transport/identity
 * and a durable approval store. This executor does not provide either.
 */
public class GraphAgentExecutor implements AgentExecutor {

	private static final Logger LOGGER = LoggerFactory.getLogger(GraphAgentExecutor.class);

	private static final Set<String> IGNORE_NODE_TYPE = Set.of("preLlm", "postLlm", "preTool", "tool", "postTool");

	private static final Set<String> BRIDGE_METADATA = Set.of("threadId", "userId", "approvalProfile", "isStreaming");

	private static final int MAX_APPROVALS = 10_000;

	public static final String STREAMING_METADATA_KEY = "isStreaming";

	private final Agent executeAgent;

	private final Clock clock;

	private final Duration approvalTtl;

	private final Duration runTimeout;

	// Retain consumed receipts as well, so identical retries cannot repeat effects.
	private final ConcurrentMap<String, PendingApproval> approvals = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, PendingApproval> taskApprovals = new ConcurrentHashMap<>();

	private final Object[] threadLocks = new Object[64];

	public GraphAgentExecutor(Agent executeAgent) {
		this(executeAgent, Clock.systemUTC(), Duration.ofMinutes(15), Duration.ofMinutes(2));
	}

	GraphAgentExecutor(Agent executeAgent, Clock clock, Duration approvalTtl, Duration runTimeout) {
		this.executeAgent = executeAgent;
		this.clock = clock;
		this.approvalTtl = approvalTtl;
		this.runTimeout = runTimeout;
		for (int i = 0; i < threadLocks.length; i++) {
			threadLocks[i] = new Object();
		}
	}

	@Override
	public void execute(RequestContext context, EventQueue eventQueue) throws JSONRPCError {
		Map<String, Object> metadata = metadata(context);
		Object profile = metadata.get(ApprovalProtocol.PROFILE_METADATA_KEY);
		if (profile != null && !ApprovalProtocol.PROFILE.equals(profile)) {
			throw invalid("Unsupported approval profile");
		}
		// Wire metadata must never create a graph resume or arbitrary state update.
		if (metadata.containsKey(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY)
				|| metadata.containsKey(RunnableConfig.STATE_UPDATE_METADATA_KEY)
				|| metadata.containsKey("checkPointId") || metadata.containsKey("checkpointId")
				|| metadata.containsKey("nextNode")) {
			throw invalid("Graph control metadata is not accepted over A2A");
		}
		ApprovalDecision decision = readDecision(context.getMessage());
		if (decision != null) {
			resume(context, eventQueue, metadata, decision);
			return;
		}
		if (taskApprovals.containsKey(context.getTaskId())
				|| (context.getTask() != null && context.getTask().getStatus().state() == TaskState.INPUT_REQUIRED)) {
			throw invalid("A structured approval decision is required for this task");
		}
		boolean bridge = ApprovalProtocol.PROFILE.equals(profile);
		if (bridge) {
			requirePeer(metadata);
			if (context.getTask() != null) {
				throw invalid("Cannot restart an existing approval-profile task");
			}
		}
		RunnableConfig config = runnableConfig(context, bridge);
		Object lock = threadLock(config);
		synchronized (lock) {
			// A new message must not overwrite a paused checkpoint on the same thread.
			boolean pendingOnThread = taskApprovals.values().stream().anyMatch(p -> !p.consumed
					&& p.config.threadId().equals(config.threadId()));
			if (pendingOnThread) {
				throw invalid("This graph thread is waiting for its approval decision");
			}
			ensureTask(context, eventQueue);
			try {
				run(context, eventQueue, config, false, bridge, requireText(context.getMessage()));
			}
			catch (Exception e) {
				LOGGER.error("Agent execution failed", e);
				emit(context, eventQueue, new RunResult(TaskState.FAILED, "Agent execution failed", null));
			}
		}
	}

	private void resume(RequestContext context, EventQueue queue, Map<String, Object> metadata,
			ApprovalDecision decision) {
		if (!ApprovalProtocol.PROFILE.equals(metadata.get(ApprovalProtocol.PROFILE_METADATA_KEY))) {
			throw invalid("Approval profile must be explicitly requested");
		}
		String peer = requirePeer(metadata);
		PendingApproval pending = decision.requestId() == null ? null : approvals.get(decision.requestId());
		if (pending == null) {
			throw invalid("Approval state is missing or expired; no action was resumed");
		}
		synchronized (threadLock(pending.config)) {
			synchronized (pending) {
				validateBinding(context, metadata, decision, peer, pending);
				List<ToolFeedback> feedbacks = validateDecisions(decision, pending);
				Map<String, String> choices = new HashMap<>();
				feedbacks.forEach(feedback -> choices.put(feedback.getId(), feedback.getResult().name()));
				if (pending.consumed) {
					if (!choices.equals(pending.choices)) {
						throw invalid("This approval request has already been consumed with a different decision");
					}
					if (pending.result == null) {
						throw invalid("The prior resume has no verified result; it will not be repeated");
					}
					if (pending.result.state == TaskState.COMPLETED) {
						new TaskUpdater(context, queue).addArtifact(List.of(new TextPart(pending.result.text)),
								UUID.randomUUID().toString(), "conversation_result", Map.of("replayed", true));
					}
					emit(context, queue, pending.result);
					return;
				}
				if (!clock.instant().isBefore(pending.expiresAt)) {
					throw invalid("Approval request expired; no action was resumed");
				}
				if (taskApprovals.get(context.getTaskId()) != pending) {
					throw invalid("Approval request is no longer current");
				}
				String currentCheckpoint;
				try {
					currentCheckpoint = executeAgent.getCurrentState(pending.config.withCheckPointId(null))
							.config().checkPointId().orElseThrow();
				}
				catch (Exception e) {
					throw invalid("Approval checkpoint is unavailable; no action was resumed");
				}
				if (!pending.checkpointId.equals(currentCheckpoint)) {
					throw invalid("Approval checkpoint changed; no action was resumed");
				}
				InterruptionMetadata feedback = InterruptionMetadata.builder(pending.interruption)
						.toolFeedbacks(feedbacks).build();
				RunnableConfig resumeConfig = RunnableConfig.builder(pending.config)
						.checkPointId(pending.checkpointId).addMetadata(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY, feedback)
						.build();
				// Consume under the entry lock before running any business action. Failures,
				// including uncertain timeouts, are never automatically rerun.
				pending.choices = Map.copyOf(choices);
				pending.consumed = true;
				try {
					pending.result = run(context, queue, resumeConfig, true, true, "");
				}
				catch (Exception e) {
					LOGGER.error("Approved graph resume failed", e);
					pending.result = new RunResult(TaskState.FAILED, "Approved graph resume failed; it will not be repeated", null);
					emit(context, queue, pending.result);
				}
			}
		}
	}

	private void validateBinding(RequestContext context, Map<String, Object> metadata, ApprovalDecision decision,
			String peer, PendingApproval pending) {
		ApprovalRequest request = pending.request;
		if (!ApprovalProtocol.PROFILE.equals(decision.profile()) || !pending.peer.equals(peer)
				|| !request.taskId().equals(decision.taskId()) || !request.contextId().equals(decision.contextId())
				|| !request.taskId().equals(context.getTaskId()) || !request.contextId().equals(context.getContextId())
				|| !request.taskId().equals(context.getMessage().getTaskId())
				|| !request.contextId().equals(context.getMessage().getContextId())) {
			throw invalid("Approval decision does not match its original request binding");
		}
		Object threadId = metadata.get("threadId");
		if (threadId != null && !pending.config.threadId().orElseThrow().equals(threadId)) {
			throw invalid("Approval decision does not match its original graph thread");
		}
	}

	private List<ToolFeedback> validateDecisions(ApprovalDecision decision, PendingApproval pending) {
		if (decision.decisions() == null || decision.decisions().size() != pending.request.tools().size()) {
			throw invalid("Exactly one decision for every proposed tool is required");
		}
		Map<String, ToolDecision> byId = new HashMap<>();
		for (ToolDecision choice : decision.decisions()) {
			if (choice == null || choice.id() == null || byId.putIfAbsent(choice.id(), choice) != null) {
				throw invalid("Duplicate or missing tool decision id");
			}
			if (!"APPROVED".equals(choice.result()) && !"REJECTED".equals(choice.result())) {
				throw invalid("Only explicit APPROVED or REJECTED decisions are supported");
			}
		}
		List<ToolFeedback> result = new ArrayList<>();
		for (ToolFeedback original : pending.interruption.toolFeedbacks()) {
			ToolDecision choice = byId.get(original.getId());
			if (choice == null || !Objects.equals(original.getName(), choice.name())
					|| !ApprovalProtocol.sha256(original.getArguments()).equals(choice.argumentsSha256())) {
				throw invalid("Tool decision does not match the proposed id, name and arguments");
			}
			result.add(ToolFeedback.builder(original).result(FeedbackResult.valueOf(choice.result())).build());
		}
		return result;
	}

	private RunResult run(RequestContext context, EventQueue queue, RunnableConfig config, boolean resume,
			boolean bridge, String input) throws Exception {
		TaskUpdater updater = new TaskUpdater(context, queue);
		updater.startWork();
		boolean streaming = Boolean.TRUE.equals(metadata(context).get(STREAMING_METADATA_KEY));
		RunnableConfig executionConfig = RunnableConfig.builder(config).addMetadata("_stream_", streaming).build();
		Flux<NodeOutput> outputs = resume ? executeAgent.stream(Map.of(), executionConfig)
				: executeAgent.stream(input, executionConfig);
		AtomicReference<InterruptionMetadata> interruption = new AtomicReference<>();
		StringBuilder chunks = new StringBuilder();
		// Wait only for this graph run. INPUT_REQUIRED closes this interaction immediately;
		// no executor thread polls task state while a human is deciding.
		NodeOutput last = outputs.doOnNext(output -> {
			if (output instanceof InterruptionMetadata paused) {
				interruption.set(paused);
			}
			else if (streaming && !output.isSTART() && !output.isEND() && !IGNORE_NODE_TYPE.contains(output.node())
					&& output instanceof StreamingOutput<?> chunk && StringUtils.hasLength(chunk.chunk())) {
				chunks.append(chunk.chunk());
				updater.addArtifact(List.of(new TextPart(chunk.chunk())), null, "conversation_result", Map.of());
			}
		}).takeUntil(output -> output instanceof InterruptionMetadata).blockLast(runTimeout);
		RunResult result;
		if (interruption.get() != null) {
			if (!bridge) {
				throw new IllegalStateException("Remote approval requires the explicit supported approval profile");
			}
			result = pause(context, config, interruption.get());
		}
		else {
			if (last == null) {
				throw new IllegalStateException("Agent returned no output");
			}
			String outputText = chunks.toString();
			if (!streaming || !StringUtils.hasLength(outputText)) {
				String outputKey = executeAgent instanceof BaseAgent agent ? agent.getOutputKey() : null;
				outputText = outputKey != null && last.state().data().containsKey(outputKey)
						? String.valueOf(last.state().data().get(outputKey)) : "No output key in result.";
				updater.addArtifact(List.of(new TextPart(outputText)), UUID.randomUUID().toString(),
						"conversation_result", Map.of("output", outputText));
			}
			result = new RunResult(TaskState.COMPLETED, outputText, null);
		}
		emit(context, queue, result);
		return result;
	}

	private RunResult pause(RequestContext context, RunnableConfig config, InterruptionMetadata interruption)
			throws Exception {
		String peer = requirePeer(metadata(context));
		StateSnapshot snapshot = executeAgent.getCurrentState(config.withCheckPointId(null));
		String checkpoint = snapshot.config().checkPointId().orElseThrow();
		Set<String> ids = new HashSet<>();
		List<ToolProposal> tools = new ArrayList<>();
		for (ToolFeedback tool : interruption.toolFeedbacks()) {
			if (!StringUtils.hasText(tool.getId()) || !StringUtils.hasText(tool.getName()) || !ids.add(tool.getId())) {
				throw new IllegalStateException("Approval interruption has invalid tool ids");
			}
			tools.add(new ToolProposal(tool.getId(), tool.getName(), tool.getArguments(),
					ApprovalProtocol.sha256(tool.getArguments()), tool.getDescription()));
		}
		for (var automaticallyApproved : interruption.getToolsAutomaticallyApproved()) {
			if (!StringUtils.hasText(automaticallyApproved.id()) || !ids.add(automaticallyApproved.id())) {
				throw new IllegalStateException("Approval interruption has ambiguous tool ids");
			}
		}
		if (tools.isEmpty() || approvals.size() >= MAX_APPROVALS) {
			throw new IllegalStateException("Approval state cannot be retained safely");
		}
		Instant expiresAt = clock.instant().plus(approvalTtl);
		ApprovalRequest request = new ApprovalRequest(ApprovalProtocol.PROFILE, UUID.randomUUID().toString(),
				context.getTaskId(), context.getContextId(), expiresAt.toString(), List.copyOf(tools));
		// Rebuild the config without transient feedback: a later pause belongs to a new decision.
		RunnableConfig.Builder storedBuilder = RunnableConfig.builder().threadId(config.threadId().orElseThrow());
		config.metadata().orElse(Map.of()).forEach((key, value) -> {
			if (BRIDGE_METADATA.contains(key)) {
				storedBuilder.addMetadata(key, value);
			}
		});
		RunnableConfig storedConfig = storedBuilder.build();
		PendingApproval pending = new PendingApproval(request, interruption, storedConfig, checkpoint, peer, expiresAt);
		approvals.put(request.requestId(), pending);
		taskApprovals.put(context.getTaskId(), pending);
		return new RunResult(TaskState.INPUT_REQUIRED, null, request);
	}

	private void emit(RequestContext context, EventQueue queue, RunResult result) {
		List<Part<?>> parts = result.request == null ? List.of(new TextPart(result.text))
				: List.of(new DataPart(ApprovalProtocol.envelope(ApprovalProtocol.REQUEST_TYPE, result.request)));
		Message message = new TaskUpdater(context, queue).newAgentMessage(parts, Map.of());
		// In SDK 0.2.5 INPUT_REQUIRED is nonterminal for the task but final for this
		// interaction. TaskUpdater cannot express that, hence the explicit event.
		queue.enqueueEvent(new TaskStatusUpdateEvent(context.getTaskId(), new TaskStatus(result.state, message, null),
				context.getContextId(), true, Map.of()));
	}

	private ApprovalDecision readDecision(Message message) {
		ApprovalDecision decision = null;
		for (Part<?> part : message.getParts()) {
			if (part instanceof DataPart data) {
				if (!ApprovalProtocol.DECISION_TYPE.equals(data.getData().get("type")) || decision != null
						|| message.getParts().size() != 1) {
					throw invalid("Expected one structured approval decision and no other message parts");
				}
				try {
					decision = ApprovalProtocol.parse(data.getData().get("payload"), ApprovalDecision.class);
					if (decision == null) {
						throw invalid("Missing approval decision payload");
					}
				}
				catch (IllegalArgumentException e) {
					throw invalid("Malformed approval decision payload");
				}
			}
		}
		return decision;
	}

	private RunnableConfig runnableConfig(RequestContext context, boolean bridge) {
		Map<String, Object> metadata = metadata(context);
		Object threadValue = metadata.get("threadId");
		String threadId = threadValue instanceof String value && StringUtils.hasText(value) ? value
				: "a2a-task-" + context.getTaskId();
		RunnableConfig.Builder builder = RunnableConfig.builder().threadId(threadId);
		metadata.forEach((key, value) -> {
			if (!bridge || BRIDGE_METADATA.contains(key)) {
				builder.addMetadata(key, value);
			}
		});
		return builder.build();
	}

	private Object threadLock(RunnableConfig config) {
		return threadLocks[Math.floorMod(config.threadId().orElseThrow().hashCode(), threadLocks.length)];
	}

	private Map<String, Object> metadata(RequestContext context) {
		return context.getParams().metadata() == null ? Map.of() : context.getParams().metadata();
	}

	private String requirePeer(Map<String, Object> metadata) {
		if (!(metadata.get("userId") instanceof String peer) || !StringUtils.hasText(peer)) {
			throw invalid("Approval profile requires a trusted-peer userId binding label");
		}
		return peer;
	}

	private String requireText(Message message) {
		StringBuilder text = new StringBuilder();
		for (Part<?> part : message.getParts()) {
			if (part instanceof TextPart value) {
				text.append(value.getText()).append('\n');
			}
		}
		return text.toString().trim();
	}

	private void ensureTask(RequestContext context, EventQueue queue) {
		if (context.getTask() == null) {
			queue.enqueueEvent(new Task(context.getTaskId(), context.getContextId(), new TaskStatus(TaskState.SUBMITTED),
					null, List.of(context.getMessage()), null));
		}
	}

	private InvalidParamsError invalid(String message) {
		return new InvalidParamsError(message);
	}

	@Override
	public void cancel(RequestContext context, EventQueue eventQueue) throws JSONRPCError {
		PendingApproval pending = taskApprovals.get(context.getTaskId());
		if (pending == null) {
			throw new TaskNotCancelableError(TaskNotCancelableError.DEFAULT_CODE, "Only a paused approval task can be canceled by this executor", null);
		}
		synchronized (threadLock(pending.config)) {
			synchronized (pending) {
				if (pending.consumed) {
					throw new TaskNotCancelableError(TaskNotCancelableError.DEFAULT_CODE, "The approval has already been consumed; an executing action cannot be canceled", null);
				}
				pending.consumed = true;
				pending.result = new RunResult(TaskState.CANCELED, "Approval task canceled", null);
				emit(context, eventQueue, pending.result);
			}
		}
	}

	private record RunResult(TaskState state, String text, ApprovalRequest request) {
	}

	private static final class PendingApproval {

		private final ApprovalRequest request;

		private final InterruptionMetadata interruption;

		private final RunnableConfig config;

		private final String checkpointId;

		private final String peer;

		private final Instant expiresAt;

		private volatile boolean consumed;

		private Map<String, String> choices;

		private RunResult result;

		private PendingApproval(ApprovalRequest request, InterruptionMetadata interruption, RunnableConfig config,
				String checkpointId, String peer, Instant expiresAt) {
			this.request = request;
			this.interruption = interruption;
			this.config = config;
			this.checkpointId = checkpointId;
			this.peer = peer;
			this.expiresAt = expiresAt;
		}

	}

}
