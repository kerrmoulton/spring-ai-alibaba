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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.a2a.server.requesthandlers.JSONRPCHandler;
import io.a2a.spec.InvalidParamsError;
import io.a2a.spec.JSONRPCResponse;
import io.a2a.spec.SendStreamingMessageRequest;
import io.a2a.spec.SendStreamingMessageResponse;
import io.a2a.spec.TaskResubscriptionRequest;
import io.a2a.spec.TaskState;
import io.a2a.spec.TaskStatus;
import io.a2a.spec.TaskStatusUpdateEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.FlowAdapters;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JsonRpcA2aRequestHandlerStreamTest {

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void errorResponseClosesInteractionAndCancelsNeverEndingSdkPublisher(boolean resubscribe) {
		JSONRPCHandler sdk = mock(JSONRPCHandler.class);
		AtomicBoolean canceled = new AtomicBoolean();
		InvalidParamsError error = new InvalidParamsError("Invalid approval; pending task unchanged");
		SendStreamingMessageResponse response = new SendStreamingMessageResponse("rpc-1", error);
		// A real publisher, deliberately left open after its error envelope, reproduces
		// the SDK's behavior when a decision is rejected before graph execution.
		Flux<SendStreamingMessageResponse> source = Flux.concat(Flux.just(response), Flux.never())
				.doOnCancel(() -> canceled.set(true));
		when(sdk.onMessageSendStream(any(SendStreamingMessageRequest.class)))
				.thenReturn(FlowAdapters.toFlowPublisher(source));
		when(sdk.onResubscribeToTask(any(TaskResubscriptionRequest.class)))
				.thenReturn(FlowAdapters.toFlowPublisher(source));

		Flux<?> result = (Flux<?>) new JsonRpcA2aRequestHandler(sdk).onHandler(body(resubscribe), null);
		List<?> events = result.collectList().block(Duration.ofSeconds(1));

		assertThat(events).hasSize(1);
		assertThat(events.get(0)).isSameAs(response);
		assertThat(((JSONRPCResponse<?>) events.get(0)).getError()).isSameAs(error);
		assertThat(((JSONRPCResponse<?>) events.get(0)).getResult()).isNull();
		assertThat(canceled).isTrue();
	}

	@Test
	void errorIsDeliveredAfterPriorEventsAndLaterEventsCannotLeakThrough() {
		JSONRPCHandler sdk = mock(JSONRPCHandler.class);
		SendStreamingMessageResponse working = status(TaskState.WORKING, false);
		SendStreamingMessageResponse error = new SendStreamingMessageResponse("rpc-1", new InvalidParamsError());
		SendStreamingMessageResponse impossibleCompletion = status(TaskState.COMPLETED, true);
		when(sdk.onMessageSendStream(any(SendStreamingMessageRequest.class)))
				.thenReturn(FlowAdapters.toFlowPublisher(Flux.just(working, error, impossibleCompletion)));

		Flux<?> result = (Flux<?>) new JsonRpcA2aRequestHandler(sdk).onHandler(body(false), null);
		List<?> events = result.collectList().block(Duration.ofSeconds(1));

		assertThat(events).hasSize(2);
		assertThat(events.get(0)).isSameAs(working);
		assertThat(events.get(1)).isSameAs(error);
	}

	@ParameterizedTest
	@ValueSource(strings = { "INPUT_REQUIRED", "COMPLETED" })
	void normalStatusResponsesRemainUnchanged(String state) {
		JSONRPCHandler sdk = mock(JSONRPCHandler.class);
		SendStreamingMessageResponse response = status(TaskState.valueOf(state), true);
		when(sdk.onMessageSendStream(any(SendStreamingMessageRequest.class)))
				.thenReturn(FlowAdapters.toFlowPublisher(Flux.just(response)));

		Flux<?> result = (Flux<?>) new JsonRpcA2aRequestHandler(sdk).onHandler(body(false), null);
		List<?> events = result.collectList().block(Duration.ofSeconds(1));

		assertThat(events).hasSize(1);
		assertThat(events.get(0)).isSameAs(response);
		assertThat(((TaskStatusUpdateEvent) response.getResult()).getStatus().state()).isEqualTo(TaskState.valueOf(state));
	}

	private SendStreamingMessageResponse status(TaskState state, boolean isFinal) {
		return new SendStreamingMessageResponse("rpc-1", new TaskStatusUpdateEvent("task-1", new TaskStatus(state),
				"context-1", isFinal, Map.of()));
	}

	private String body(boolean resubscribe) {
		if (resubscribe) {
			return """
					{"jsonrpc":"2.0","id":"rpc-1","method":"tasks/resubscribe","params":{"id":"task-1"}}
					""";
		}
		return """
				{"jsonrpc":"2.0","id":"rpc-1","method":"message/stream","params":{
				  "message":{"kind":"message","role":"user","messageId":"message-1",
				    "parts":[{"kind":"text","text":"hello"}]}}}
				""";
	}

}
