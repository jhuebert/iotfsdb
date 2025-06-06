package org.huebert.iotfsdb.mcp;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
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
                NULL
                """)
            .build();
    }

    @PostMapping("/chat")
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
