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
package com.alibaba.cloud.ai.agent.studio.dto.messages;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.alibaba.cloud.ai.agent.studio.dto.messages.ToolRequestConfirmMessageDTO.ToolFeedback;
import com.alibaba.cloud.ai.agent.studio.dto.messages.ToolRequestConfirmMessageDTO.ToolFeedback.FeedbackResult;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.ai.chat.messages.AssistantMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ToolRequestConfirmMessageDTOTest {

	private final ObjectMapper mapper = new ObjectMapper();

	@ParameterizedTest
	@MethodSource("invalidFeedbacks")
	void rejectsMissingOrAmbiguousDecisions(List<ToolFeedback> feedbacks) {
		ToolRequestConfirmMessageDTO dto = new ToolRequestConfirmMessageDTO();
		dto.setToolFeedback(feedbacks);

		assertThrows(IllegalArgumentException.class,
				() -> ToolRequestConfirmMessageDTO.convertToolFeedbacks(feedbacks));
		assertThrows(IllegalArgumentException.class,
				() -> dto.toInterruptionMetadata("tools", new OverAllState()));
	}

	static Stream<Arguments> invalidFeedbacks() {
		ToolFeedback missingResult = feedback("call-1", "write", null);
		return Stream.of(
				Arguments.of((Object) null),
				Arguments.of(List.of()),
				Arguments.of(Arrays.asList((ToolFeedback) null)),
				Arguments.of(List.of(missingResult)),
				Arguments.of(Arrays.asList(feedback("call-1", "write", FeedbackResult.APPROVED), null)),
				Arguments.of(List.of(feedback("call-1", "wrong-name", FeedbackResult.APPROVED),
						feedback("call-1", "write", FeedbackResult.REJECTED))));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "\t\n", "\u2003" })
	void rejectsBlankIdsAndNames(String value) {
		assertThrows(IllegalArgumentException.class,
				() -> convert(feedback(value, "write", FeedbackResult.APPROVED)));
		assertThrows(IllegalArgumentException.class,
				() -> convert(feedback("call-1", value, FeedbackResult.APPROVED)));
	}

	@ParameterizedTest
	@EnumSource(FeedbackResult.class)
	void preservesExplicitLocalDecisions(FeedbackResult result) throws Exception {
		ToolFeedback feedback = feedback("call-1", "write", result);
		feedback.setArguments("{\"amount\":7}");
		feedback.setDescription("Human supplied decision");
		ToolRequestConfirmMessageDTO dto = new ToolRequestConfirmMessageDTO();
		dto.setToolFeedback(List.of(feedback));
		dto.setMetadata(Map.of("request", "req-1"));
		dto.setToolsAutomaticallyApproved(List.of(new ToolRequestConfirmMessageDTO.ToolCallDTO(
				new AssistantMessage.ToolCall("read-1", "function", "read", "{}"))));

		InterruptionMetadata converted = dto.toInterruptionMetadata("tools", new OverAllState());
		InterruptionMetadata.ToolFeedback decision = converted.toolFeedbacks().get(0);
		assertEquals("call-1", decision.getId());
		assertEquals("write", decision.getName());
		assertEquals(feedback.getArguments(), decision.getArguments());
		assertEquals(feedback.getDescription(), decision.getDescription());
		assertEquals(result.name(), decision.getResult().name());
		assertEquals("req-1", converted.metadata("request").orElseThrow());
		assertEquals("read-1", converted.getToolsAutomaticallyApproved().get(0).id());
		assertEquals(result, mapper.readValue(mapper.writeValueAsString(feedback), ToolFeedback.class).getResult());
	}

	@ParameterizedTest
	@ValueSource(strings = { "{}", "{\"result\":null}" })
	void absentJsonResultNeverApproves(String json) throws Exception {
		ToolFeedback feedback = mapper.readValue(json, ToolFeedback.class);
		feedback.setId("call-1");
		feedback.setName("write");
		assertThrows(IllegalArgumentException.class, () -> convert(feedback));
	}

	@ParameterizedTest
	@ValueSource(strings = { "\"UNKNOWN\"", "\"approved\"", "\"\"", "\" \"", "0", "1", "2", "-1", "true", "{}", "[]" })
	void rejectsUnknownJsonResultsAndNumericOrdinals(String result) {
		assertThrows(JsonProcessingException.class,
				() -> mapper.readValue("{\"result\":" + result + "}", ToolFeedback.class));
	}

	@Test
	void pendingRequestCanBeRenderedButCannotBeSubmittedWithoutDecision() {
		InterruptionMetadata pending = InterruptionMetadata.builder("tools", new OverAllState())
				.addToolFeedback(new InterruptionMetadata.ToolFeedback("call-1", "write", "{}", null, null))
				.build();
		ToolRequestConfirmMessageDTO dto = new ToolRequestConfirmMessageDTO(pending);
		assertNull(dto.getToolFeedback().get(0).getResult());
		assertThrows(IllegalArgumentException.class,
				() -> dto.toInterruptionMetadata("tools", new OverAllState()));
	}

	private static InterruptionMetadata convert(ToolFeedback feedback) {
		ToolRequestConfirmMessageDTO dto = new ToolRequestConfirmMessageDTO();
		dto.setToolFeedback(List.of(feedback));
		return dto.toInterruptionMetadata("tools", new OverAllState());
	}

	private static ToolFeedback feedback(String id, String name, FeedbackResult result) {
		ToolFeedback feedback = new ToolFeedback();
		feedback.setId(id);
		feedback.setName(name);
		feedback.setArguments("{}");
		feedback.setResult(result);
		return feedback;
	}
}
