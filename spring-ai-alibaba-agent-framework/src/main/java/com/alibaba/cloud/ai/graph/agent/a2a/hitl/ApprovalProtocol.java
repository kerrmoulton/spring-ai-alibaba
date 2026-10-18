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
package com.alibaba.cloud.ai.graph.agent.a2a.hitl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Opt-in, application-level approval profile for the A2A Java SDK 0.2.5.Beta2.
 * This is not an A2A 1.0 extension or an authentication protocol. Only the proposed
 * tool calls are sent over the wire; graph state and checkpoints stay on the server.
 */
public final class ApprovalProtocol {

	public static final String PROFILE = "spring-ai-alibaba-hitl/0.2.5-v1";

	public static final String REQUEST_TYPE = "saa.approval.request";

	public static final String DECISION_TYPE = "saa.approval.decision";

	public static final String PROFILE_METADATA_KEY = "approvalProfile";

	private static final ObjectMapper MAPPER = new ObjectMapper()
			.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
			.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
			.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

	private ApprovalProtocol() {
	}

	public record ToolProposal(String id, String name, String arguments, String argumentsSha256, String description) {
	}

	public record ApprovalRequest(String profile, String requestId, String taskId, String contextId, String expiresAt,
			List<ToolProposal> tools) {
	}

	public record ToolDecision(String id, String name, String argumentsSha256, String result) {
	}

	public record ApprovalDecision(String profile, String requestId, String taskId, String contextId,
			List<ToolDecision> decisions) {
	}

	public static Map<String, Object> envelope(String type, Object payload) {
		return Map.of("type", type, "payload", MAPPER.convertValue(payload, new TypeReference<Map<String, Object>>() { }));
	}

	public static <T> T parse(Object payload, Class<T> type) {
		return MAPPER.convertValue(payload, type);
	}

	/**
	 * Hash parsed JSON with recursively sorted object keys. Arrays preserve their order.
	 * Invalid/ambiguous JSON is rejected, rather than hashing unverified raw arguments.
	 */
	public static String sha256(String arguments) {
		try {
			JsonNode parsed = MAPPER.readTree(arguments);
			if (parsed == null) {
				throw new IllegalArgumentException("Tool arguments must contain JSON");
			}
			String canonical = MAPPER.writeValueAsString(canonicalize(parsed));
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(canonical.getBytes(StandardCharsets.UTF_8)));
		}
		catch (JsonProcessingException | NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Cannot canonicalize tool arguments", e);
		}
	}

	private static JsonNode canonicalize(JsonNode node) {
		if (node.isObject()) {
			Map<String, JsonNode> sorted = new TreeMap<>();
			node.fields().forEachRemaining(entry -> sorted.put(entry.getKey(), canonicalize(entry.getValue())));
			ObjectNode result = MAPPER.createObjectNode();
			sorted.forEach(result::set);
			return result;
		}
		if (node.isArray()) {
			ArrayNode result = MAPPER.createArrayNode();
			node.forEach(value -> result.add(canonicalize(value)));
			return result;
		}
		return node;
	}

}
