/*
 * Copyright 2025-2026 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package repro;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import com.alibaba.cloud.ai.graph.*;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.agent.*;
import com.alibaba.cloud.ai.graph.agent.a2a.A2aRemoteAgent;
import com.alibaba.cloud.ai.graph.agent.a2a.AgentCardProvider;
import com.alibaba.cloud.ai.graph.agent.a2a.AgentCardWrapper;
import com.alibaba.cloud.ai.graph.agent.flow.agent.SequentialAgent;
import com.alibaba.cloud.ai.a2a.autoconfigure.A2aServerProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;
import com.alibaba.cloud.ai.graph.agent.hook.hip.*;
import com.alibaba.cloud.ai.graph.checkpoint.savers.MemorySaver;
import com.alibaba.cloud.ai.graph.exception.GraphRunnerException;
import com.alibaba.cloud.ai.graph.internal.node.Node;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.agent.studio.loader.AgentLoader;
import com.alibaba.cloud.ai.a2a.core.server.*;
import com.alibaba.cloud.ai.a2a.core.route.*;
import io.a2a.server.events.*;
import io.a2a.server.requesthandlers.*;
import io.a2a.server.tasks.*;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentCapabilities;
import org.springframework.ai.chat.messages.*;
import org.springframework.ai.chat.model.*;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.function.FunctionToolCallback;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.function.*;
import reactor.core.publisher.Flux;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableConfigurationProperties(A2aServerProperties.class)
public class ReproApplication {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String BASE = System.getProperty("business.url", "http://127.0.0.1:18080");
    static final Path EVIDENCE = Path.of(System.getProperty("evidence.dir", "evidence"));
    static final List<Map<String,Object>> AUDIT = new CopyOnWriteArrayList<>();
    static final Map<String,Map<String,Integer>> COUNTS = new ConcurrentHashMap<>();
    public static void main(String[] args) { SpringApplication.run(ReproApplication.class, args); }

    static synchronized void record(String layer, Object value) {
        Map<String,Object> row = new LinkedHashMap<>();
        row.put("time", Instant.now().toString()); row.put("layer", layer); row.put("value", value);
        AUDIT.add(row);
        try { Files.createDirectories(EVIDENCE); Files.writeString(EVIDENCE.resolve("runtime.jsonl"), JSON.writeValueAsString(row)+"\n", java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND); }
        catch(Exception e) { throw new RuntimeException(e); }
    }
    static Map<String,Object> nodeView(NodeOutput n) {
        Map<String,Object> v = new LinkedHashMap<>();
        v.put("class",n.getClass().getName());v.put("node",n.node());v.put("agent",n.agent());
        if(n instanceof InterruptionMetadata im) v.put("toolFeedbacks", im.toolFeedbacks());
        if(n instanceof StreamingOutput<?> so) {v.put("chunk",so.chunk());v.put("outputType",so.getOutputType());}
        return v;
    }
    @Bean ChatModel mockModel() { return new ScriptedModel(); }
    public record ReadInput(String caseId, String customerId) {}
    public record CreateInput(String caseId, String customerId, String sku, int quantity) {}
    static String http(String method, String path, Object body) {
        try {
            HttpRequest.Builder r = HttpRequest.newBuilder(URI.create(BASE+path)).timeout(java.time.Duration.ofSeconds(15));
            if(body==null) r.GET(); else r.header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
            HttpResponse<String> response=HttpClient.newHttpClient().send(r.build(),HttpResponse.BodyHandlers.ofString());
            if(response.statusCode()!=200) throw new IllegalStateException("Business API: "+response.statusCode()+" "+response.body());
            return response.body();
        } catch(Exception e) {throw new RuntimeException(e);}
    }
    static List<ToolCallback> businessTools() {
        return List.of(
            FunctionToolCallback.builder("read_customer", (ReadInput a) -> http("GET", "/business/customer?caseId="+URLEncoder.encode(a.caseId(),StandardCharsets.UTF_8)+"&customerId="+URLEncoder.encode(a.customerId(),StandardCharsets.UTF_8),null))
                .description("Query a simulated customer; read-only").inputType(ReadInput.class).build(),
            FunctionToolCallback.builder("create_order", (CreateInput a) -> http("POST", "/business/orders",a))
                .description("Create a simulated order; writes exactly once per actual tool execution").inputType(CreateInput.class).build());
    }
    static ReactAgent businessAgent(String name, ChatModel model) {
        return ReactAgent.builder().name(name).description("Same deterministic model, tools, approval policy and MemorySaver")
            .model(model).tools(businessTools()).saver(new MemorySaver()).outputKey("answer")
            .hooks(HumanInTheLoopHook.builder().approvalOn(Map.of("create_order",ToolConfig.builder().description("模拟写操作：创建一笔订单。批准后才调用本地业务API。").build())).build())
            .build();
    }
    @Bean("serverAgent") @Profile("remote")
    ReactAgent serverAgent(ChatModel model) { return businessAgent("server_business",model); }

    @Bean("localAgent") @Profile("client")
    SequentialAgent localAgent(ChatModel model) {
        return SequentialAgent.builder().name("local_sequential").description("Local nested HITL baseline")
            .subAgents(List.of(businessAgent("local_business", model))).saver(new MemorySaver()).build();
    }

    @Bean("remoteAgent") @Profile("client")
    A2aRemoteAgent remoteAgent(AgentCardProvider provider) {
        // name is the Nacos lookup key, not the UI parent name. No static AgentCard fallback.
        AgentCardProvider observed = new AgentCardProvider() {
            public boolean supportGetAgentCardByName() { return provider.supportGetAgentCardByName(); }
            public AgentCardWrapper getAgentCard() { return auditCard(provider.getAgentCard()); }
            public AgentCardWrapper getAgentCard(String name) { return auditCard(provider.getAgentCard(name)); }
            private AgentCardWrapper auditCard(AgentCardWrapper card) {
                record("nacos.discovery", Map.of("provider", provider.getClass().getName(),
                    "lookupName", "server_business", "cardName", card.name(), "cardUrl", card.url()));
                return card; // exact delegate result; passive observation only
            }
        };
        A2aRemoteAgent remote = A2aRemoteAgent.builder().name("server_business")
            .description("Nacos discovered remote business agent").agentCardProvider(observed)
            .instruction("{input}").shareState(false).outputKey("answer")
            .compileConfig(CompileConfig.builder().saverConfig(new com.alibaba.cloud.ai.graph.checkpoint.config.SaverConfig().register(new MemorySaver())).build()).build();
        return remote;
    }

    @Bean("a2aParent") @Profile("client")
    SequentialAgent a2aParent(@Qualifier("remoteAgent") A2aRemoteAgent remote) {
        return SequentialAgent.builder().name("a2a_sequential").description("Sequential -> Nacos discovered A2A -> business")
            .subAgents(List.of(remote)).saver(new MemorySaver()).build();
    }

    @Bean @Profile("client")
    AgentLoader visibleAgents(@Qualifier("localAgent") SequentialAgent local,@Qualifier("a2aParent") SequentialAgent remote) {
        Map<String,Agent> map=Map.of(local.name(),local,remote.name(),remote);
        return new AgentLoader() {
            public List<String> listAgents(){return map.keySet().stream().sorted().toList();}
            public Agent loadAgent(String name){return Objects.requireNonNull(map.get(name),"Unknown agent: "+name);}
        };
    }
    @Bean(destroyMethod="shutdownNow") @Profile("remote") ExecutorService a2aExecutor(){return Executors.newCachedThreadPool();}
    @Bean @Profile("remote") RouterFunction<ServerResponse> a2aRouter(AgentCard card,@Qualifier("serverAgent") ReactAgent server,ExecutorService a2aExecutor) {
        var store=new InMemoryPushNotificationConfigStore();
        var handler=new DefaultRequestHandler(new GraphAgentExecutor(new ObservedAgent(server)),new InMemoryTaskStore(),new InMemoryQueueManager(),store,new BasePushNotificationSender(store),a2aExecutor);
        var observed=new JsonRpcA2aRequestHandler(new JSONRPCHandler(card,handler)) {
            @Override public Object onHandler(String body,ServerRequest.Headers headers) {
                record("a2a.request",body);
                Object result=super.onHandler(body,headers);
                if(result instanceof Flux<?> flux) return flux.doOnNext(value -> record("a2a.event",toTree(value)));
                record("a2a.event",toTree(result));return result;
            }
        };
        return new JsonRpcA2aRouterProvider().getRouter(observed);
    }
    static Object toTree(Object o) {
        try{return io.a2a.util.Utils.OBJECT_MAPPER.readTree(io.a2a.util.Utils.OBJECT_MAPPER.writeValueAsString(o));}
        catch(Exception e){return String.valueOf(o);}
    }
    /** Passive observation only. Every original NodeOutput is forwarded unchanged. */
    static class ObservedAgent extends BaseAgent {
        final ReactAgent delegate;
        ObservedAgent(ReactAgent d){super(d.name(),d.description(),true,false,d.getOutputKey(),null);delegate=d;}
        protected StateGraph initGraph(){return delegate.getGraph();}
        public Node asNode(boolean a,boolean b){return delegate.asNode(a,b);}
        @Override public Flux<NodeOutput> stream(String input,RunnableConfig config) throws GraphRunnerException {
            return delegate.stream(input,config).doOnNext(n -> record("remote.graph",Map.of("threadId",config.threadId().orElse(""),"output",nodeView(n))));
        }
        @Override public Optional<OverAllState> invoke(String input,RunnableConfig config) throws GraphRunnerException {return delegate.invoke(input,config);}
    }
    /** The mock controls only model output, never hooks, tools, graph results or UI. */
    static class ScriptedModel implements ChatModel {
        static final Pattern CASE=Pattern.compile("case=([A-Za-z0-9_-]+)");
        @Override public ChatResponse call(Prompt prompt) {
            List<Message> messages=prompt.getInstructions();
            String user="create order case=interactive";int lastUser=-1;
            for(int i=0;i<messages.size();i++) if(messages.get(i) instanceof UserMessage u && u.getText()!=null && !u.getText().isBlank()){user=u.getText();lastUser=i;}
            var matcher=CASE.matcher(user);String caseId=matcher.find()?matcher.group(1):"interactive";
            boolean readOnly=user.toLowerCase(Locale.ROOT).contains("read only")||user.contains("只查询");
            String lastTool=null;String lastData=null;
            for(int i=Math.max(0,lastUser);i<messages.size();i++) if(messages.get(i) instanceof ToolResponseMessage t)
                for(var r:t.getResponses()){lastTool=r.name();lastData=r.responseData();}
            AssistantMessage answer;
            if("create_order".equals(lastTool)) answer=new AssistantMessage(lastData!=null&&lastData.contains("rejected by human")?"已拒绝。未创建订单。":"订单已创建。业务 API 返回："+lastData);
            else if("read_customer".equals(lastTool)&&readOnly) answer=new AssistantMessage("查询完成："+lastData);
            else {
                String tool=lastTool==null?"read_customer":"create_order";
                Object args=lastTool==null?new ReadInput(caseId,"C100"):new CreateInput(caseId,"C100","DEMO-001",1);
                try{answer=AssistantMessage.builder().content("").toolCalls(List.of(new AssistantMessage.ToolCall(caseId+"-"+tool,"function",tool,JSON.writeValueAsString(args)))).build();}
                catch(Exception e){throw new RuntimeException(e);}
            }
            record("mock.model",Map.of("caseId",caseId,"toolCalls",answer.getToolCalls(),"text",Objects.toString(answer.getText(),"")));
            return new ChatResponse(List.of(new Generation(answer)));
        }
        @Override public Flux<ChatResponse> stream(Prompt p){return Flux.defer(()->Flux.just(call(p)));}
    }
    @RestController static class BusinessApi {
        @GetMapping("/business/customer") Map<String,Object> read(@RequestParam String caseId,@RequestParam String customerId){
            increment(caseId,"reads");record("business.read",Map.of("caseId",caseId,"customerId",customerId));
            return Map.of("caseId",caseId,"customerId",customerId,"customerName","模拟客户","status","active");
        }
        @PostMapping("/business/orders") Map<String,Object> create(@RequestBody CreateInput a){
            increment(a.caseId(),"writes");String id=UUID.randomUUID().toString();
            var v=Map.<String,Object>of("caseId",a.caseId(),"orderId",id,"customerId",a.customerId(),"sku",a.sku(),"quantity",a.quantity());
            record("business.write",v);return v;
        }
        static void increment(String id,String name){COUNTS.computeIfAbsent(id,k->new ConcurrentHashMap<>(Map.of("reads",0,"writes",0))).merge(name,1,Integer::sum);}
        @GetMapping("/repro/status") Map<String,Object> status(){return Map.of("frameworkCommit","17e26ff6fe951d605771c1845229ca3edb043634","counts",COUNTS,"events",AUDIT.size(),"nativeStudio","/chatui/","businessApiBase",BASE,"model","deterministic local mock; no paid model");}
        @GetMapping("/repro/audit") List<Map<String,Object>> audit(){return AUDIT;}
    }
}
