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
package com.alibaba.cloud.ai.agent.studio.dto.messages;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;

import org.springframework.ai.chat.messages.AssistantMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data Transfer Object for AssistantMessage.
 * Provides serialization-friendly representation with default constructor and getters/setters.
 */
public class ToolRequestConfirmMessageDTO implements MessageDTO {

	@JsonProperty("messageType")
	private String messageType = "tool-confirm";

	@JsonProperty("content")
	private String content;

	@JsonProperty("metadata")
	private Map<String, Object> metadata;

	@JsonProperty("toolFeedback")
	private List<ToolFeedback> toolFeedback;

	@JsonProperty("toolsAutomaticallyApproved")
	private List<ToolCallDTO> toolsAutomaticallyApproved;

	/**
	 * Default constructor for deserialization.
	 */
	public ToolRequestConfirmMessageDTO() {
		this.metadata = new HashMap<>();
		this.toolFeedback = new ArrayList<>();
		this.toolsAutomaticallyApproved = new ArrayList<>();
	}

	/**
	 * Constructor with content.
	 */
	public ToolRequestConfirmMessageDTO(String content) {
		this();
		this.content = content;
	}

	/**
	 * Constructor from InterruptionMetadata.
	 */
	public ToolRequestConfirmMessageDTO(InterruptionMetadata interruptionMetadata) {
		this();
		this.content = "";
		this.metadata = new HashMap<>(interruptionMetadata.metadata().orElse(Map.of()));

		// Convert InterruptionMetadata.ToolFeedback to ToolFeedback
		if (interruptionMetadata.toolFeedbacks() != null && !interruptionMetadata.toolFeedbacks().isEmpty()) {
			this.toolFeedback = new ArrayList<>();
			for (InterruptionMetadata.ToolFeedback feedback : interruptionMetadata.toolFeedbacks()) {
				ToolFeedback toolFeedback = new ToolFeedback();
				toolFeedback.setId(feedback.getId());
				toolFeedback.setName(feedback.getName());
				toolFeedback.setArguments(feedback.getArguments());
				toolFeedback.setDescription(feedback.getDescription());

				// Convert FeedbackResult
				if (feedback.getResult() != null) {
					toolFeedback.setResult(ToolFeedback.FeedbackResult.valueOf(feedback.getResult().name()));
				}

				this.toolFeedback.add(toolFeedback);
			}
		}

		// Convert toolsAutomaticallyApproved
		if (interruptionMetadata.getToolsAutomaticallyApproved() != null
				&& !interruptionMetadata.getToolsAutomaticallyApproved().isEmpty()) {
			this.toolsAutomaticallyApproved = new ArrayList<>();
			for (AssistantMessage.ToolCall toolCall : interruptionMetadata.getToolsAutomaticallyApproved()) {
				this.toolsAutomaticallyApproved.add(new ToolCallDTO(toolCall));
			}
		}
	}

	/**
	 * Convert to InterruptionMetadata.
	 * @param nodeId the node ID where the interruption occurred
	 * @param state the overall state at the time of interruption
	 */
	public InterruptionMetadata toInterruptionMetadata(String nodeId, OverAllState state) {
		List<InterruptionMetadata.ToolFeedback> feedbacks = convertToolFeedbacks(this.toolFeedback);
		InterruptionMetadata.Builder builder = InterruptionMetadata.builder(nodeId, state)
				.toolFeedbacks(feedbacks);

		// Set metadata
		if (this.metadata != null) {
			for (Map.Entry<String, Object> entry : this.metadata.entrySet()) {
				builder.addMetadata(entry.getKey(), entry.getValue());
			}
		}

		// Convert toolsAutomaticallyApproved
		if (this.toolsAutomaticallyApproved != null && !this.toolsAutomaticallyApproved.isEmpty()) {
			List<AssistantMessage.ToolCall> springToolCalls = new ArrayList<>();
			for (ToolCallDTO toolCallDTO : this.toolsAutomaticallyApproved) {
				springToolCalls.add(toolCallDTO.toToolCall());
			}
			builder.toolsAutomaticallyApproved(springToolCalls);
		}

		return builder.build();
	}

	/**
	 * Validate and convert explicit human decisions. Pending tool requests may have no
	 * result when rendered, but must never be treated as approved when submitted.
	 * @param toolFeedbacks the decisions submitted for the interrupted tool calls
	 * @return the validated decisions, in submission order
	 * @throws IllegalArgumentException if any decision is missing or ambiguous
	 */
	public static List<InterruptionMetadata.ToolFeedback> convertToolFeedbacks(List<ToolFeedback> toolFeedbacks) {
		if (toolFeedbacks == null || toolFeedbacks.isEmpty()) {
			throw new IllegalArgumentException("toolFeedbacks must contain at least one explicit decision");
		}

		Set<String> toolIds = new HashSet<>();
		List<InterruptionMetadata.ToolFeedback> feedbacks = new ArrayList<>();
		for (int index = 0; index < toolFeedbacks.size(); index++) {
			ToolFeedback feedback = toolFeedbacks.get(index);
			String field = "toolFeedbacks[" + index + "]";
			if (feedback == null) {
				throw new IllegalArgumentException(field + " cannot be null");
			}
			if (feedback.getId() == null || feedback.getId().isBlank()) {
				throw new IllegalArgumentException(field + ".id cannot be null or blank");
			}
			if (!toolIds.add(feedback.getId())) {
				throw new IllegalArgumentException(field + ".id must be unique");
			}
			if (feedback.getName() == null || feedback.getName().isBlank()) {
				throw new IllegalArgumentException(field + ".name cannot be null or blank");
			}
			if (feedback.getResult() == null) {
				throw new IllegalArgumentException(field + ".result must be APPROVED, REJECTED, or EDITED");
			}

			feedbacks.add(new InterruptionMetadata.ToolFeedback(feedback.getId(), feedback.getName(),
					feedback.getArguments(),
					InterruptionMetadata.ToolFeedback.FeedbackResult.valueOf(feedback.getResult().name()),
					feedback.getDescription()));
		}
		return feedbacks;
	}

	// Getters and Setters

	public String getMessageType() {
		return messageType;
	}

	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Map<String, Object> getMetadata() {
		return metadata;
	}

	public void setMetadata(Map<String, Object> metadata) {
		this.metadata = metadata;
	}

	public List<ToolFeedback> getToolFeedback() {
		return toolFeedback;
	}

	public void setToolFeedback(List<ToolFeedback> toolFeedback) {
		this.toolFeedback = toolFeedback;
	}

	public List<ToolCallDTO> getToolsAutomaticallyApproved() {
		return toolsAutomaticallyApproved;
	}

	public void setToolsAutomaticallyApproved(List<ToolCallDTO> toolsAutomaticallyApproved) {
		this.toolsAutomaticallyApproved = toolsAutomaticallyApproved;
	}

	/**
	 * DTO for ToolFeedback.
	 */
	public static class ToolFeedback {
		@JsonProperty("id")
		private String id;

		@JsonProperty("name")
		private String name;

		@JsonProperty("arguments")
		private String arguments;

		@JsonProperty("result")
		private FeedbackResult result;

		@JsonProperty("description")
		private String description;

		public ToolFeedback() {
		}

		// Getters and Setters

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getArguments() {
			return arguments;
		}

		public void setArguments(String arguments) {
			this.arguments = arguments;
		}

		public FeedbackResult getResult() {
			return result;
		}

		public void setResult(FeedbackResult result) {
			this.result = result;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * Enum for ToolFeedback result.
		 */
		public enum FeedbackResult {
			APPROVED,
			REJECTED,
			EDITED;

			@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
			public static FeedbackResult fromValue(String value) {
				if (value == null) {
					throw new IllegalArgumentException("result must be APPROVED, REJECTED, or EDITED");
				}
				return FeedbackResult.valueOf(value);
			}
		}
	}

	/**
	 * DTO for ToolCall within AssistantMessage.
	 */
	public static class ToolCallDTO {
		@JsonProperty("id")
		private String id;

		@JsonProperty("type")
		private String type;

		@JsonProperty("name")
		private String name;

		@JsonProperty("arguments")
		private String arguments;

		public ToolCallDTO() {
		}

		public ToolCallDTO(AssistantMessage.ToolCall toolCall) {
			this.id = toolCall.id();
			this.type = toolCall.type();
			this.name = toolCall.name();
			this.arguments = toolCall.arguments();
		}

		public AssistantMessage.ToolCall toToolCall() {
			return new AssistantMessage.ToolCall(this.id, this.type, this.name, this.arguments);
		}

		// Getters and Setters

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getArguments() {
			return arguments;
		}

		public void setArguments(String arguments) {
			this.arguments = arguments;
		}
	}
}

