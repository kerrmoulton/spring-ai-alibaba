/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.graph.agent.a2a;

import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalDecision;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalRequest;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ToolDecision;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ToolProposal;
import com.alibaba.cloud.ai.graph.streaming.OutputType;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.ai.chat.messages.AssistantMessage;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Opt-in, single-parent/single-remote approval continuation profile for A2A 0.2.5.
 * Correlation is intentionally process-local: losing this object fails closed. The peer
 * userId label is not authenticated human identity. Production deployments require a
 * trusted principal boundary, durable storage and business-side idempotency.
 */
final class A2aApprovalClient {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() { };
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    Map<String, Object> apply(AgentCardWrapper card, String agentName, String outputKey,
            OverAllState state, RunnableConfig parentConfig, RunnableConfig remoteConfig,
            boolean streaming, Supplier<String> instruction) {
        String parentThread = parentConfig.threadId().orElseThrow(() -> new IllegalArgumentException("Approval bridge requires parent threadId"));
        String user = userId(parentConfig);
        String key = card.url() + "\n" + agentName + "\n" + parentThread;
        // The fixed profile allows one pending invocation for this parent/node. No state is
        // taken from the browser to choose an arbitrary remote URL or task.
        if (!sessions.containsKey(key) && sessions.size() >= 10_000) {
            throw new IllegalStateException("Approval correlation capacity reached; start a fresh isolated client");
        }
        Session session = sessions.computeIfAbsent(key, ignored -> new Session(user));
        String resultKey = outputKey == null || outputKey.isBlank() ? "messages" : outputKey;
        Flux<GraphResponse<NodeOutput>> flux = Flux.defer(() -> {
            try {
                synchronized (session) {
                    return Flux.fromIterable(exchange(session, card, agentName, resultKey, state,
                            parentConfig, remoteConfig, streaming, instruction, user));
                }
            }
            catch (Exception e) {
                return Flux.error(e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
        return Map.of(resultKey, flux);
    }

    private List<GraphResponse<NodeOutput>> exchange(Session session, AgentCardWrapper card,
            String agentName, String outputKey, OverAllState state, RunnableConfig parentConfig,
            RunnableConfig remoteConfig, boolean streaming, Supplier<String> instruction, String user) throws Exception {
        if (!session.userId.equals(user)) {
            throw new IllegalArgumentException("Approval belongs to another userId label");
        }
        Object feedbackValue = parentConfig.metadata(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY).orElse(null);
        InterruptionMetadata feedback = null;
        if (feedbackValue != null) {
            if (!(feedbackValue instanceof InterruptionMetadata typed)) {
                throw new IllegalArgumentException("Approval feedback must be typed InterruptionMetadata");
            }
            feedback = typed;
        }
        if (feedback != null && session.request == null) {
            throw new IllegalStateException("No pending remote approval; correlation may have been lost");
        }
        if (session.request != null && !session.completed && feedback == null) {
            return List.of(GraphResponse.done(interruption(session.request, agentName, state)));
        }

        ApprovalDecision decision = null;
        if (feedback != null) {
            decision = decision(session.request, feedback);
            String fingerprint = ApprovalProtocol.sha256(JSON.writeValueAsString(decision));
            if (session.decisionFingerprint != null && !session.decisionFingerprint.equals(fingerprint)) {
                throw new IllegalArgumentException("A different decision was already submitted for this approval");
            }
            if (session.completed) {
                return List.of(GraphResponse.done(Map.of(outputKey, session.completedText)));
            }
            // Set before I/O: after an uncertain transport outcome only the identical
            // decision may be retried. The remote also atomically deduplicates it.
            session.decisionFingerprint = fingerprint;
        }
        else {
            session.request = null;
            session.decisionFingerprint = null;
            session.completed = false;
        }

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("kind", "message");
        message.put("role", "user");
        message.put("messageId", UUID.randomUUID().toString());
        if (decision != null) {
            message.put("taskId", decision.taskId());
            message.put("contextId", decision.contextId());
            message.put("parts", List.of(Map.of("kind", "data", "data",
                    ApprovalProtocol.envelope(ApprovalProtocol.DECISION_TYPE, decision))));
        }
        else {
            message.put("parts", List.of(Map.of("kind", "text", "text", instruction.get())));
        }
        Map<String, Object> metadata = Map.of("threadId", remoteConfig.threadId().orElseThrow(),
                "userId", user, "approvalProfile", ApprovalProtocol.PROFILE);
        String rpcId = UUID.randomUUID().toString();
        Map<String, Object> payload = Map.of("jsonrpc", "2.0", "id", rpcId,
                "method", streaming ? "message/stream" : "message/send",
                "params", Map.of("message", message, "metadata", metadata));
        Response result = request(card.url(), JSON.writeValueAsString(payload), streaming, rpcId);
        if (result.taskId == null || result.taskId.isBlank() || result.contextId == null || result.contextId.isBlank()) {
            throw new IllegalArgumentException("Remote response lacks task/context correlation");
        }
        if (decision != null && (!decision.taskId().equals(result.taskId) || !decision.contextId().equals(result.contextId))) {
            throw new IllegalArgumentException("Resume response does not belong to the original task/context");
        }
        List<GraphResponse<NodeOutput>> outputs = new ArrayList<>();
        for (String chunk : result.chunks) {
            outputs.add(GraphResponse.of(new StreamingOutput<>(new AssistantMessage(chunk), chunk,
                    "A2aNode", agentName, state, OutputType.AGENT_MODEL_STREAMING)));
        }
        if (result.approval != null) {
            validateRequest(result.approval, result.taskId, result.contextId);
            if (decision != null && result.approval.requestId().equals(decision.requestId())) {
                throw new IllegalStateException("Remote did not consume the submitted decision");
            }
            // Store the authoritative correlation before returning the graph control
            // value. NodeExecutor intentionally does not merge partial state on interrupt.
            session.request = result.approval;
            session.decisionFingerprint = null;
            outputs.add(GraphResponse.done(interruption(result.approval, agentName, state)));
        }
        else {
            if (!result.completed) {
                throw new IllegalStateException("Remote stream ended without completion or a valid approval request");
            }
            session.completed = true;
            session.completedText = String.join("", result.chunks);
            outputs.add(GraphResponse.done(Map.of(outputKey, session.completedText)));
        }
        return outputs;
    }

    private static String userId(RunnableConfig config) {
        Object value = config.metadata("userId").orElseGet(() -> config.metadata("user_id").orElse(null));
        if (!(value instanceof String user) || user.isBlank()) {
            throw new IllegalArgumentException("Approval bridge requires a nonempty trusted-peer userId label");
        }
        return user;
    }

    private static String feedbackId(ApprovalRequest request, ToolProposal tool) {
        return request.requestId() + ":" + tool.id();
    }

    private static InterruptionMetadata interruption(ApprovalRequest request, String agentName, OverAllState state) {
        var builder = InterruptionMetadata.builder("A2aNode", state)
                .addMetadata("approvalProfile", request.profile())
                .addMetadata("approvalRequestId", request.requestId())
                .addMetadata("remoteTaskId", request.taskId())
                .addMetadata("remoteContextId", request.contextId())
                .addMetadata("remoteAgent", agentName);
        for (ToolProposal tool : request.tools()) {
            builder.addToolFeedback(InterruptionMetadata.ToolFeedback.builder()
                    .id(feedbackId(request, tool)).name(tool.name()).arguments(tool.arguments())
                    .description("Remote agent " + agentName + ": " + tool.description()).build());
        }
        return builder.build();
    }

    private static ApprovalDecision decision(ApprovalRequest request, InterruptionMetadata feedback) {
        if (Instant.parse(request.expiresAt()).isBefore(Instant.now())) {
            throw new IllegalArgumentException("Approval request expired");
        }
        if (feedback.toolFeedbacks().size() != request.tools().size()) {
            throw new IllegalArgumentException("Decision must cover exactly the pending tools");
        }
        Map<String, InterruptionMetadata.ToolFeedback> received = new HashMap<>();
        for (var item : feedback.toolFeedbacks()) {
            if (item == null || item.getId() == null || received.putIfAbsent(item.getId(), item) != null) {
                throw new IllegalArgumentException("Duplicate or missing tool decision id");
            }
        }
        List<ToolDecision> decisions = new ArrayList<>();
        for (ToolProposal proposal : request.tools()) {
            var item = received.get(feedbackId(request, proposal));
            if (item == null || !proposal.name().equals(item.getName()) || item.getArguments() == null
                    || !proposal.argumentsSha256().equals(ApprovalProtocol.sha256(item.getArguments()))) {
                throw new IllegalArgumentException("Decision does not match the exact pending proposal");
            }
            if (item.getResult() != InterruptionMetadata.ToolFeedback.FeedbackResult.APPROVED
                    && item.getResult() != InterruptionMetadata.ToolFeedback.FeedbackResult.REJECTED) {
                throw new IllegalArgumentException("This approval profile only supports explicit APPROVED or REJECTED");
            }
            decisions.add(new ToolDecision(proposal.id(), proposal.name(), proposal.argumentsSha256(), item.getResult().name()));
        }
        return new ApprovalDecision(ApprovalProtocol.PROFILE, request.requestId(), request.taskId(), request.contextId(), decisions);
    }

    private static void validateRequest(ApprovalRequest request, String taskId, String contextId) {
        if (!ApprovalProtocol.PROFILE.equals(request.profile()) || request.requestId() == null || request.requestId().isBlank()
                || taskId == null || contextId == null || !taskId.equals(request.taskId()) || !contextId.equals(request.contextId())
                || request.expiresAt() == null || !Instant.parse(request.expiresAt()).isAfter(Instant.now())
                || request.tools() == null || request.tools().isEmpty()) {
            throw new IllegalArgumentException("Invalid or unsupported remote approval request");
        }
        var ids = new HashSet<String>();
        for (var tool : request.tools()) {
            if (tool == null || tool.id() == null || tool.id().isBlank() || !ids.add(tool.id())
                    || tool.name() == null || tool.name().isBlank() || tool.arguments() == null
                    || !Objects.equals(tool.argumentsSha256(), ApprovalProtocol.sha256(tool.arguments()))) {
                throw new IllegalArgumentException("Invalid remote tool proposal");
            }
        }
    }

    private static Response request(String url, String payload, boolean streaming, String rpcId) throws Exception {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("AgentCard.url is empty");
        }
        var config = RequestConfig.custom().setConnectTimeout(5000).setConnectionRequestTimeout(5000).setSocketTimeout(30000).build();
        try (var client = HttpClients.custom().setDefaultRequestConfig(config).build()) {
            HttpPost post = new HttpPost(url);
            post.setHeader("Accept", streaming ? "text/event-stream" : "application/json");
            post.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
            try (var response = client.execute(post)) {
                if (response.getStatusLine().getStatusCode() != 200 || response.getEntity() == null) {
                    throw new IllegalStateException("A2A HTTP failure: " + response.getStatusLine());
                }
                var result = new Response(rpcId);
                String type = response.getEntity().getContentType() == null ? "" : response.getEntity().getContentType().getValue();
                if (type.contains("text/event-stream")) {
                    try (var reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
                        String line;
                        StringBuilder data = new StringBuilder();
                        while ((line = reader.readLine()) != null) {
                            if (line.isEmpty()) {
                                if (data.length() > 0) { result.accept(data.toString()); data.setLength(0); }
                            }
                            else if (line.startsWith("data:")) {
                                if (data.length() > 0) { data.append('\n'); }
                                data.append(line.substring(5).stripLeading());
                            }
                        }
                        if (data.length() > 0) { result.accept(data.toString()); }
                    }
                }
                else {
                    result.accept(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
                }
                return result;
            }
        }
    }

    private static final class Session {
        private final String userId;
        private ApprovalRequest request;
        private String decisionFingerprint;
        private boolean completed;
        private String completedText = "";
        private Session(String userId) { this.userId = userId; }
    }

    private static final class Response {
        private final List<String> chunks = new ArrayList<>();
        private final String rpcId;
        private boolean interactionFinal;
        private Response(String rpcId) { this.rpcId = rpcId; }
        private ApprovalRequest approval;
        private String taskId;
        private String contextId;
        private boolean completed;

        @SuppressWarnings("unchecked")
        private void accept(String data) throws Exception {
            if ("[DONE]".equals(data)) { return; }
            if (interactionFinal) { throw new IllegalArgumentException("Remote emitted data after the final status"); }
            Map<String, Object> envelope = JSON.readValue(data, MAP);
            if (!rpcId.equals(envelope.get("id"))) { throw new IllegalArgumentException("JSON-RPC response id mismatch"); }
            if (envelope.get("error") != null) {
                throw new IllegalStateException("A2A JSON-RPC error: " + envelope.get("error"));
            }
            if (!(envelope.get("result") instanceof Map<?, ?> raw)) {
                throw new IllegalArgumentException("Missing A2A result");
            }
            Map<String, Object> result = (Map<String, Object>) raw;
            String kind = (String) result.get("kind");
            String id = "task".equals(kind) ? (String) result.get("id") : (String) result.get("taskId");
            String context = (String) result.get("contextId");
            if (id != null) {
                if (taskId != null && !taskId.equals(id)) { throw new IllegalArgumentException("Remote task changed within one response"); }
                taskId = id;
            }
            if (context != null) {
                if (contextId != null && !contextId.equals(context)) { throw new IllegalArgumentException("Remote context changed within one response"); }
                contextId = context;
            }
            if (result.get("artifact") instanceof Map<?, ?> artifact) { textParts((Map<String, Object>) artifact); }
            if (result.get("artifacts") instanceof List<?> artifacts) {
                for (Object artifact : artifacts) { textParts((Map<String, Object>) artifact); }
            }
            if (result.get("status") instanceof Map<?, ?> statusRaw) {
                Map<String, Object> status = (Map<String, Object>) statusRaw;
                String state = (String) status.get("state");
                if ("input-required".equals(state)) {
                    if (completed || approval != null) { throw new IllegalArgumentException("Conflicting remote pause events"); }
                    if (!(status.get("message") instanceof Map<?, ?> message)) { throw new IllegalArgumentException("Approval status lacks message"); }
                    Object parts = message.get("parts");
                    if (!(parts instanceof List<?> list)) { throw new IllegalArgumentException("Approval status lacks parts"); }
                    for (Object part : list) {
                        if (part instanceof Map<?, ?> map && map.get("data") instanceof Map<?, ?> content
                                && ApprovalProtocol.REQUEST_TYPE.equals(content.get("type"))) {
                            if (approval != null) { throw new IllegalArgumentException("Multiple remote approval envelopes"); }
                            approval = JSON.convertValue(content.get("payload"), ApprovalRequest.class);
                        }
                    }
                    if (approval == null) { throw new IllegalArgumentException("Unsupported unstructured approval status"); }
                    if ("status-update".equals(kind) && !Boolean.TRUE.equals(result.get("final"))) {
                        throw new IllegalArgumentException("Approval pause must close this interaction");
                    }
                    interactionFinal = true;
                }
                else if ("completed".equals(state)) {
                    if (approval != null) { throw new IllegalArgumentException("Remote incorrectly completed a paused task"); }
                    if (chunks.isEmpty() && status.get("message") instanceof Map<?, ?> message) {
                        textParts((Map<String, Object>) message);
                    }
                    completed = true;
                    interactionFinal = true;
                }
                else if (!List.of("submitted", "working").contains(state)) {
                    throw new IllegalStateException("Remote task did not succeed: " + state);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void textParts(Map<String, Object> container) {
            if (container.get("parts") instanceof List<?> parts) {
                for (Object part : parts) {
                    if (part instanceof Map<?, ?> map && map.get("text") instanceof String text && !text.isEmpty()) {
                        chunks.add(text);
                    }
                }
            }
        }
    }
}
