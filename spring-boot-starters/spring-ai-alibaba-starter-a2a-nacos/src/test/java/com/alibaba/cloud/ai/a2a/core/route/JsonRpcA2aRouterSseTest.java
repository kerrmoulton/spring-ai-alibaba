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
package com.alibaba.cloud.ai.a2a.core.route;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.cloud.ai.a2a.core.server.JsonRpcA2aRequestHandler;
import io.a2a.server.requesthandlers.JSONRPCHandler;
import io.a2a.spec.InvalidParamsError;
import io.a2a.spec.SendStreamingMessageRequest;
import io.a2a.spec.SendStreamingMessageResponse;
import io.a2a.spec.TaskState;
import io.a2a.spec.TaskStatus;
import io.a2a.spec.TaskStatusUpdateEvent;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.FlowAdapters;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockAsyncContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JsonRpcA2aRouterSseTest {

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void jsonRpcErrorClosesActualSpringSseResponseWithoutFailingTask(boolean multiAgent) throws Exception {
		AtomicBoolean canceled = new AtomicBoolean();
		SendStreamingMessageResponse error = new SendStreamingMessageResponse("rpc-1", new InvalidParamsError("bad decision"));
		Flux<SendStreamingMessageResponse> sdkPublisher = Flux.concat(Flux.just(error), Flux.never())
				.doOnCancel(() -> canceled.set(true));
		MockMvc mvc = mvc(sdkPublisher, multiAgent);

		MvcResult initial = perform(mvc, multiAgent);
		// This observes completion through Spring MVC's SSE/async adapter. A Flux-only
		// test misses a router which emits the error but never completes its HTTP body.
		assertThat(initial.getAsyncResult(1000)).isNull();
		MvcResult completed = mvc.perform(asyncDispatch(initial)).andExpect(status().isOk()).andReturn();

		assertThat(completed.getResponse().getContentAsString()).contains("bad decision", "\"error\"")
				.doesNotContain("\"state\":\"failed\"");
		assertThat(canceled).isTrue();
	}

	@ParameterizedTest
	@ValueSource(strings = { "INPUT_REQUIRED", "COMPLETED" })
	void finalTaskStatusCompletesHttpAndCancelsSource(String state) throws Exception {
		AtomicBoolean canceled = new AtomicBoolean();
		SendStreamingMessageResponse response = new SendStreamingMessageResponse("rpc-1",
				new TaskStatusUpdateEvent("task-1", new TaskStatus(TaskState.valueOf(state)), "context-1", true, Map.of()));
		MockMvc mvc = mvc(Flux.concat(Flux.just(response), Flux.never()).doOnCancel(() -> canceled.set(true)), false);

		MvcResult initial = perform(mvc, false);
		assertThat(initial.getAsyncResult(1000)).isNull();
		mvc.perform(asyncDispatch(initial)).andExpect(status().isOk());
		assertThat(canceled).isTrue();
	}

	@Test
	void synchronousTerminalSignalBeforeSubscribeReturnsStillClosesAndDisposes() throws Exception {
		AtomicBoolean canceled = new AtomicBoolean();
		SendStreamingMessageResponse error = new SendStreamingMessageResponse("rpc-1", new InvalidParamsError("sync error"));
		Flux<SendStreamingMessageResponse> synchronous = Flux.concat(Flux.just(error), Flux.never())
				.doOnCancel(() -> canceled.set(true));
		// Direct helper invocation deliberately bypasses the request handler's delayed
		// subscription, so completion occurs before subscription.update receives its value.
		MockMvc mvc = MockMvcBuilders.routerFunctions(RouterFunctions.route()
				.POST("/a2a", request -> JsonRpcA2aRouterProvider.sseResponse(synchronous)).build()).build();
		MvcResult initial = perform(mvc, false);
		assertThat(initial.getAsyncResult(1000)).isNull();
		mvc.perform(asyncDispatch(initial)).andExpect(status().isOk());
		assertThat(canceled).isTrue();
	}

	@Test
	void emptyPublisherCompletionReachesHttp() throws Exception {
		MockMvc mvc = mvc(Flux.empty(), false);
		MvcResult initial = perform(mvc, false);
		assertThat(initial.getAsyncResult(1000)).isNull();
		mvc.perform(asyncDispatch(initial)).andExpect(status().isOk());
	}

	@Test
	void publisherFailureReachesSpringAsyncError() throws Exception {
		IllegalStateException failure = new IllegalStateException("upstream failure");
		MockMvc mvc = mvc(Flux.error(failure), false);
		MvcResult initial = perform(mvc, false);
		assertThat(initial.getAsyncResult(1000)).isSameAs(failure);
	}

	@ParameterizedTest
	@ValueSource(strings = { "completion", "timeout", "error" })
	void servletLifecycleDisposesUpstreamSubscription(String lifecycle) throws Exception {
		AtomicBoolean canceled = new AtomicBoolean();
		CountDownLatch subscribed = new CountDownLatch(1);
		Flux<SendStreamingMessageResponse> source = Flux.<SendStreamingMessageResponse>never()
				.doOnSubscribe(subscription -> subscribed.countDown()).doOnCancel(() -> canceled.set(true));
		MockMvc mvc = mvc(source, false);
		MvcResult initial = perform(mvc, false);
		assertThat(subscribed.await(1, TimeUnit.SECONDS)).isTrue();
		MockAsyncContext asyncContext = (MockAsyncContext) initial.getRequest().getAsyncContext();
		for (AsyncListener listener : asyncContext.getListeners()) {
			switch (lifecycle) {
				case "completion" -> listener.onComplete(new AsyncEvent(asyncContext));
				case "timeout" -> listener.onTimeout(new AsyncEvent(asyncContext));
				case "error" -> listener.onError(new AsyncEvent(asyncContext, new IOException("client disconnected")));
				default -> throw new IllegalArgumentException(lifecycle);
			}
		}
		assertThat(canceled).isTrue();
	}

	private MockMvc mvc(Flux<SendStreamingMessageResponse> source, boolean multiAgent) {
		JSONRPCHandler sdk = mock(JSONRPCHandler.class);
		when(sdk.onMessageSendStream(any(SendStreamingMessageRequest.class)))
				.thenReturn(FlowAdapters.toFlowPublisher(source));
		JsonRpcA2aRequestHandler handler = new JsonRpcA2aRequestHandler(sdk);
		RouterFunction<ServerResponse> route;
		if (multiAgent) {
			MultiAgentRequestRouter router = new MultiAgentRequestRouter();
			router.registerHandler("writer", handler);
			route = new MultiAgentJsonRpcRouterProvider(router).getRouter();
		}
		else {
			route = new JsonRpcA2aRouterProvider().getRouter(handler);
		}
		return MockMvcBuilders.routerFunctions(route).build();
	}

	private MvcResult perform(MockMvc mvc, boolean multiAgent) throws Exception {
		return mvc.perform(post(multiAgent ? "/a2a/writer" : "/a2a").contentType(MediaType.APPLICATION_JSON)
				.content("""
						{"jsonrpc":"2.0","id":"rpc-1","method":"message/stream","params":{
						  "message":{"kind":"message","role":"user","messageId":"message-1",
						    "parts":[{"kind":"text","text":"hello"}]}}}
						"""))
				.andExpect(request().asyncStarted()).andReturn();
	}

}
