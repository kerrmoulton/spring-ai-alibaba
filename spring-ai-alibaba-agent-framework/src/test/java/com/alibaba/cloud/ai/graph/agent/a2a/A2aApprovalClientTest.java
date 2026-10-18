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

import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata.ToolFeedback;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata.ToolFeedback.FeedbackResult;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalDecision;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ApprovalRequest;
import com.alibaba.cloud.ai.graph.agent.a2a.hitl.ApprovalProtocol.ToolProposal;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class A2aApprovalClientTest {
    private static final String ARGS = "{\"sku\":\"DEMO\",\"quantity\":1}";
    private static final ObjectMapper JSON = new ObjectMapper();

    private ApprovalRequest proposal() {
        return new ApprovalRequest(ApprovalProtocol.PROFILE, "approval-1", "task-1", "context-1",
                Instant.now().plusSeconds(300).toString(),
                List.of(new ToolProposal("call-1", "create", ARGS, ApprovalProtocol.sha256(ARGS), "Create demo")));
    }
    private ToolFeedback choice(String id, String name, String args, FeedbackResult result) {
        return ToolFeedback.builder().id(id).name(name).arguments(args).result(result).build();
    }
    private ApprovalDecision decision(ApprovalRequest request, ToolFeedback... feedbacks) throws Exception {
        var method = A2aApprovalClient.class.getDeclaredMethod("decision", ApprovalRequest.class, InterruptionMetadata.class);
        method.setAccessible(true);
        try { return (ApprovalDecision) method.invoke(null, request, InterruptionMetadata.builder().toolFeedbacks(List.of(feedbacks)).build()); }
        catch (InvocationTargetException e) { throw (Exception) e.getCause(); }
    }
    @Test void explicitApprovalKeepsExactOriginalWireIdentity() throws Exception {
        var result = decision(proposal(), choice("approval-1:call-1", "create", "{\"quantity\":1,\"sku\":\"DEMO\"}", FeedbackResult.APPROVED));
        assertEquals("call-1", result.decisions().get(0).id());
        assertEquals("task-1", result.taskId());
        assertEquals("context-1", result.contextId());
        assertEquals("APPROVED", result.decisions().get(0).result());
    }
    @Test void explicitRejectionIsSupported() throws Exception {
        assertEquals("REJECTED", decision(proposal(), choice("approval-1:call-1", "create", ARGS, FeedbackResult.REJECTED)).decisions().get(0).result());
    }
    @Test void missingResultFailsClosed() {
        assertThrows(IllegalArgumentException.class, () -> decision(proposal(), choice("approval-1:call-1", "create", ARGS, null)));
    }
    @Test void editedProposalIsNotSilentlyApproved() {
        assertThrows(IllegalArgumentException.class, () -> decision(proposal(), choice("approval-1:call-1", "create", ARGS, FeedbackResult.EDITED)));
    }
    @Test void changedApprovedArgumentsFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> decision(proposal(), choice("approval-1:call-1", "create", "{\"sku\":\"OTHER\"}", FeedbackResult.APPROVED)));
    }
    @Test void staleCardWithSameToolIdFailsClosed() {
        assertThrows(IllegalArgumentException.class, () -> decision(proposal(), choice("earlier-approval:call-1", "create", ARGS, FeedbackResult.APPROVED)));
    }
    @Test void mismatchedNameFailsClosed() {
        assertThrows(IllegalArgumentException.class, () -> decision(proposal(), choice("approval-1:call-1", "other", ARGS, FeedbackResult.APPROVED)));
    }
    @Test void missingAndExtraFeedbackFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> decision(proposal()));
        var ok = choice("approval-1:call-1", "create", ARGS, FeedbackResult.APPROVED);
        assertThrows(IllegalArgumentException.class, () -> decision(proposal(), ok, ok));
    }
    @Test void duplicateFeedbackCannotExploitHookNameIdMismatch() {
        var p = new ApprovalRequest(ApprovalProtocol.PROFILE, "approval-1", "task-1", "context-1", Instant.now().plusSeconds(300).toString(),
                List.of(proposal().tools().get(0), new ToolProposal("call-2", "delete", ARGS, ApprovalProtocol.sha256(ARGS), "Delete")));
        var forged = choice("approval-1:call-1", "wrong-name", ARGS, FeedbackResult.APPROVED);
        var rejected = choice("approval-1:call-1", "create", ARGS, FeedbackResult.REJECTED);
        assertThrows(IllegalArgumentException.class, () -> decision(p, forged, rejected));
        assertThrows(IllegalArgumentException.class, () -> decision(p, rejected, forged));
    }
    @Test void expiredRequestFailsClosed() {
        var p = proposal();
        var expired = new ApprovalRequest(p.profile(), p.requestId(), p.taskId(), p.contextId(), Instant.now().minusSeconds(1).toString(), p.tools());
        assertThrows(IllegalArgumentException.class, () -> decision(expired, choice("approval-1:call-1", "create", ARGS, FeedbackResult.APPROVED)));
    }
    private Object response() throws Exception {
        var cls = Class.forName(A2aApprovalClient.class.getName()+"$Response");
        var c = cls.getDeclaredConstructor(String.class); c.setAccessible(true); return c.newInstance("rpc-1");
    }
    private void accept(Object response, Map<String,Object> envelope) throws Exception {
        var m = response.getClass().getDeclaredMethod("accept", String.class);m.setAccessible(true);
        try { m.invoke(response, JSON.writeValueAsString(envelope)); }
        catch(InvocationTargetException e) { throw (Exception)e.getCause(); }
    }
    private Map<String,Object> status(String task, String state) {
        return Map.of("jsonrpc","2.0","id","rpc-1","result",Map.of("kind","status-update","taskId",task,"contextId","context-1","status",Map.of("state",state),"final",true));
    }
    @Test void wrongRpcIdFailsClosed() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> accept(response(), Map.of("id","other","result",Map.of("kind","task"))));
    }
    @Test void taskCannotChangeMidResponse() throws Exception {
        var r = response(); accept(r,status("task-1","working"));
        assertThrows(IllegalArgumentException.class, () -> accept(r,status("task-2","completed")));
    }
    @Test void noDataAllowedAfterFinalStatus() throws Exception {
        var r = response(); accept(r,status("task-1","completed"));
        assertThrows(IllegalArgumentException.class, () -> accept(r,status("task-1","working")));
    }
    @Test void failedTaskIsAnErrorNotAssistantSuccessText() throws Exception {
        assertThrows(IllegalStateException.class, () -> accept(response(),status("task-1","failed")));
    }
    @Test void unstructuredInputRequiredFailsClosed() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> accept(response(),status("task-1","input-required")));
    }
    @Test void unsupportedAuthorizationStateIsNotMisrepresentedAsApproval() throws Exception {
        assertThrows(IllegalStateException.class, () -> accept(response(),status("task-1","auth-required")));
    }
    @Test void malformedJsonRpcErrorDoesNotCompleteGraph() throws Exception {
        assertThrows(IllegalStateException.class, () -> accept(response(),Map.of("id","rpc-1","error",Map.of("code",-32602,"message","Bad decision"))));
    }
    @Test void reconstructedParentNodeAdaptersShareStableCorrelationOwner() throws Exception {
        var card = new io.a2a.spec.AgentCard.Builder().name("remote").description("test")
                .url("http://127.0.0.1:1/a2a").version("test").protocolVersion("0.2.5")
                .capabilities(new io.a2a.spec.AgentCapabilities.Builder().streaming(true).build())
                .defaultInputModes(List.of("text/plain")).defaultOutputModes(List.of("text/plain"))
                .skills(List.of()).build();
        var compile = com.alibaba.cloud.ai.graph.CompileConfig.builder().build();
        var agent = A2aRemoteAgent.builder().name("remote").description("test remote").agentCard(card).compileConfig(compile)
                .approvalBridge(true).outputKey("answer").instruction("test").build();
        var node = agent.asNode(true, false);
        var config = com.alibaba.cloud.ai.graph.RunnableConfig.builder().threadId("parent-thread")
                .addMetadata("user_id", "tester").build();
        // apply constructs the lazy Flux but does not send HTTP until subscription.
        node.actionFactory().apply(compile).apply(new com.alibaba.cloud.ai.graph.OverAllState(Map.of()), config).join();
        node.actionFactory().apply(compile).apply(new com.alibaba.cloud.ai.graph.OverAllState(Map.of()), config).join();
        var owner = A2aRemoteAgent.class.getDeclaredField("approvalClient"); owner.setAccessible(true);
        var sessions = A2aApprovalClient.class.getDeclaredField("sessions"); sessions.setAccessible(true);
        assertEquals(1, ((Map<?, ?>) sessions.get(owner.get(agent))).size());
    }
}
