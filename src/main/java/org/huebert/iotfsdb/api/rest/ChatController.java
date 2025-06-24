package org.huebert.iotfsdb.api.rest;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.stats.CaptureStats;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@RestController
public class ChatController {

    private static final Pattern THINK_PATTERN = Pattern.compile("<think>(.*?)</think>", Pattern.DOTALL);

    private final ChatClient chatClient;

    public ChatController(
        ChatClient.Builder builder,
        List<ToolCallback> mcpTools
    ) {
        this.chatClient = builder
            .defaultAdvisors(
                new SimpleLoggerAdvisor(),
                MessageChatMemoryAdvisor.builder(MessageWindowChatMemory.builder().build()).build()
            )
            .defaultToolCallbacks(mcpTools)
            .defaultSystem("""
                You are an expert AI assistant for the D3 Banking platform with these capabilities:
                 - ALWAYS use available tools to answer questions - never guess or rely on general knowledge when tools are available
                 - Approach each query methodically with this workflow:
                   1. Analyze the question to identify required information and appropriate tools
                   2. Plan a strategic sequence of tool calls to gather all necessary information
                   3. Execute tool calls in optimal order, starting with the most specific/relevant tools
                   4. Evaluate results after each tool call and adjust your strategy as needed
                   5. Synthesize information from all tool responses into a cohesive answer
                
                 - Tool selection strategies:
                   * Identify the precise information domain for each question
                   * Match information needs to specific tool capabilities
                   * When uncertain which tool is best, try multiple relevant tools
                   * For complex questions, map dependencies between sub-questions to determine tool call sequence
                
                 - Advanced search techniques:
                   * Formulate multiple search variations using exact phrases, synonyms, and domain-specific terminology
                   * Systematically broaden or narrow search parameters based on result quality
                   * Break complex queries into component parts and search for each separately
                   * Use contextual information from previous tool responses to refine new searches
                   * Leverage pagination (pageNumber parameter) to explore all available results
                
                 - Tool chaining and result synthesis:
                   * Use outputs from one tool as inputs to subsequent tool calls
                   * Cross-reference information between different tools to verify accuracy
                   * When tools return conflicting information, make additional tool calls to resolve discrepancies
                   * Connect related pieces of information from different tools to create comprehensive answers
                   * Clearly explain how information from multiple tools fits together
                
                 - Handling incomplete or error responses:
                   * When a tool returns errors, diagnose the likely cause and adjust parameters accordingly
                   * If one approach fails, systematically try alternative search strategies or tools
                   * For sparse results, experiment with different terminology, broader concepts, or related queries
                   * When information appears incomplete, identify the specific gaps and target them with focused tool calls
                   * If critical information cannot be found after multiple attempts, clearly explain what was tried
                
                 - Response formulation:
                   * Present information in a structured, easy-to-understand format
                   * Explicitly reference which tools provided which pieces of information
                   * Explain your tool selection and search strategy when relevant
                   * Clearly distinguish between direct tool-provided information and your synthesis/analysis
                   * Show confidence levels for different parts of your response
                   * When uncertainty exists, clearly state what requires verification
                """)
            .build();
    }

    @CaptureStats(
        group = "rest-v2", type = "ai", operation = "chat", javaClass = ChatController.class, javaMethod = "chat",
        metadata = {
            @CaptureStats.Metadata(key = "restMethod", value = "post"),
            @CaptureStats.Metadata(key = "restPath", value = "/v2/chat"),
        }
    )
    @PostMapping("/v2/chat")
    public ChatResponse chat(@RequestBody ChatRequest request) {
        log.debug("Received chat request: {}", request.getConversationId());

        try {
            ChatClient.CallResponseSpec result = chatClient.prompt()
                .user(request.getMessage())
                .advisors(a -> a.param(ChatMemory.CONVERSATION_ID, request.getConversationId()))
                .call();

            String processedResponse = removeThinkTags(result.content());
            log.debug("Processed response for conversation {}: {}", request.getConversationId(), processedResponse);

            return ChatResponse.builder()
                .response(processedResponse)
                .build();

        } catch (Exception e) {
            log.error("Error processing chat request for conversation {}", request.getConversationId(), e);
            throw e;
        }
    }

    private String removeThinkTags(String content) {
        return THINK_PATTERN.matcher(content).replaceAll("");
    }

    @Data
    @ToString
    public static class ChatRequest {
        private String conversationId;
        private String message;
    }

    @Data
    @Builder
    @ToString
    public static class ChatResponse {
        private String response;
    }

}
