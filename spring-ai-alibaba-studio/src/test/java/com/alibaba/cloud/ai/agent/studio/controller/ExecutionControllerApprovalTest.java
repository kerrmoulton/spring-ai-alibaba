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
package com.alibaba.cloud.ai.agent.studio.controller;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import com.alibaba.cloud.ai.agent.studio.dto.AgentResumeRequest;
import com.alibaba.cloud.ai.agent.studio.dto.messages.ToolRequestConfirmMessageDTO.ToolFeedback;
import com.alibaba.cloud.ai.agent.studio.dto.messages.ToolRequestConfirmMessageDTO.ToolFeedback.FeedbackResult;
import com.alibaba.cloud.ai.agent.studio.loader.AgentLoader;
import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.agent.Agent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ExecutionControllerApprovalTest {

	private final RecordingAgentLoader agentLoader = new RecordingAgentLoader();

	private final ExecutionController controller = new ExecutionController(agentLoader);

	private final ObjectMapper mapper = new ObjectMapper();

	private MockMvc mockMvc;

	@BeforeEach
	void setUp() {
		mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
	}

	@ParameterizedTest
	@MethodSource("invalidDecisionFields")
	void rejectsMalformedDecisionBeforeLoadingOrExecutingAgent(String fields) throws Exception {
		String json = "{\"appName\":\"agent\",\"threadId\":\"thread-1\",\"userId\":\"user-1\"" + fields + "}";
		AgentResumeRequest request = mapper.readValue(json, AgentResumeRequest.class);

		ResponseStatusException exception = assertThrows(ResponseStatusException.class,
				() -> controller.agentResumeSse(request).blockLast(Duration.ofSeconds(5)));
		assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
		assertHttpBadRequest(json);
		assertEquals(0, agentLoader.loadCount);
	}

	static Stream<String> invalidDecisionFields() {
		return Stream.of(
				"",
				",\"toolFeedbacks\":null",
				",\"toolFeedbacks\":[]",
				",\"toolFeedbacks\":[null]",
				",\"toolFeedbacks\":[{}]",
				decisions("{\"id\":\"call-1\",\"name\":\"write\"}"),
				decisions("{\"id\":\"call-1\",\"name\":\"write\",\"result\":null}"),
				decisions("{\"id\":null,\"name\":\"write\",\"result\":\"APPROVED\"}"),
				decisions("{\"id\":\"\",\"name\":\"write\",\"result\":\"APPROVED\"}"),
				decisions("{\"id\":\" \" ,\"name\":\"write\",\"result\":\"APPROVED\"}"),
				decisions("{\"id\":\"call-1\",\"name\":null,\"result\":\"APPROVED\"}"),
				decisions("{\"id\":\"call-1\",\"name\":\"\",\"result\":\"APPROVED\"}"),
				decisions("{\"id\":\"call-1\",\"name\":\" \" ,\"result\":\"APPROVED\"}"),
				decisions("{\"id\":\"call-1\",\"name\":\"wrong-name\",\"result\":\"APPROVED\"},"
						+ "{\"id\":\"call-1\",\"name\":\"write\",\"result\":\"REJECTED\"}"),
				decisions("{\"id\":\"call-1\",\"name\":\"write\",\"result\":\"APPROVED\"},null"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "\"UNKNOWN\"", "\"approved\"", "\"\"", "0", "1", "2", "true", "{}", "[]" })
	void invalidEnumResultsAreHttpBadRequests(String result) throws Exception {
		String json = "{\"appName\":\"agent\",\"threadId\":\"thread-1\""
				+ decisions("{\"id\":\"call-1\",\"name\":\"write\",\"result\":" + result + "}") + "}";
		assertHttpBadRequest(json);
		assertEquals(0, agentLoader.loadCount);
	}

	@ParameterizedTest
	@EnumSource(FeedbackResult.class)
	void forwardsValidLocalDecisionsUnchanged(FeedbackResult result) throws Exception {
		RecordingAgent agent = agentLoader.agent;
		AgentResumeRequest request = request(result);

		controller.agentResumeSse(request).blockLast(Duration.ofSeconds(5));

		assertEquals(1, agentLoader.loadCount);
		assertEquals(1, agent.streamCount);
		assertEquals("", agent.message);
		RunnableConfig config = agent.config;
		assertEquals("thread-1", config.threadId().orElseThrow());
		assertEquals("user-1", config.metadata("user_id").orElseThrow());
		InterruptionMetadata metadata = (InterruptionMetadata) config
				.metadata(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY).orElseThrow();
		InterruptionMetadata.ToolFeedback decision = metadata.toolFeedbacks().get(0);
		assertEquals("call-1", decision.getId());
		assertEquals("write", decision.getName());
		assertEquals("{\"amount\":7}", decision.getArguments());
		assertEquals("Human supplied decision", decision.getDescription());
		assertEquals(result.name(), decision.getResult().name());
	}

	@Test
	void preservesBadRequestFromExecutionInsteadOfWrappingAsServerError() throws Exception {
		ResponseStatusException original = new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid decision");
		agentLoader.agent.failure = original;

		ResponseStatusException actual = assertThrows(ResponseStatusException.class,
				() -> controller.agentResumeSse(request(FeedbackResult.REJECTED)).blockLast(Duration.ofSeconds(5)));
		assertSame(original, actual);
	}

	@Test
	void nullRequestIsBadRequest() {
		ResponseStatusException exception = assertThrows(ResponseStatusException.class,
				() -> controller.agentResumeSse(null).blockLast(Duration.ofSeconds(5)));
		assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
		assertEquals(0, agentLoader.loadCount);
	}

	private void assertHttpBadRequest(String json) throws Exception {
		MvcResult result = mockMvc.perform(post("/resume_sse").contentType(MediaType.APPLICATION_JSON)
				.accept(MediaType.TEXT_EVENT_STREAM).content(json)).andReturn();
		if (result.getRequest().isAsyncStarted()) {
			mockMvc.perform(asyncDispatch(result)).andExpect(status().isBadRequest());
		}
		else {
			assertEquals(HttpStatus.BAD_REQUEST.value(), result.getResponse().getStatus());
		}
	}

	private static String decisions(String feedback) {
		return ",\"toolFeedbacks\":[" + feedback + "]";
	}

	private static AgentResumeRequest request(FeedbackResult result) {
		AgentResumeRequest request = new AgentResumeRequest();
		request.appName = "agent";
		request.threadId = "thread-1";
		request.userId = "user-1";
		ToolFeedback feedback = new ToolFeedback();
		feedback.setId("call-1");
		feedback.setName("write");
		feedback.setArguments("{\"amount\":7}");
		feedback.setDescription("Human supplied decision");
		feedback.setResult(result);
		request.toolFeedbacks = List.of(feedback);
		return request;
	}

	private static class RecordingAgentLoader implements AgentLoader {

		private final RecordingAgent agent = new RecordingAgent();

		private int loadCount;

		@Override
		public List<String> listAgents() {
			return List.of("agent");
		}

		@Override
		public Agent loadAgent(String name) {
			assertEquals("agent", name);
			this.loadCount++;
			return this.agent;
		}
	}

	private static class RecordingAgent extends Agent {

		private RunnableConfig config;

		private String message;

		private int streamCount;

		private ResponseStatusException failure;

		@Override
		public Flux<NodeOutput> stream(String message, RunnableConfig config) {
			this.streamCount++;
			this.message = message;
			this.config = config;
			if (this.failure != null) {
				throw this.failure;
			}
			return Flux.empty();
		}

		@Override
		protected StateGraph initGraph() {
			throw new UnsupportedOperationException("Graph execution is not used in this test");
		}
	}

}
