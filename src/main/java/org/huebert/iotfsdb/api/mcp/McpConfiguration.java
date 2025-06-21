package org.huebert.iotfsdb.api.mcp;

import lombok.extern.slf4j.Slf4j;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Slf4j
@Configuration
public class McpConfiguration {

    private final IotfsdbProperties iotfsdbProperties;

    public McpConfiguration(IotfsdbProperties iotfsdbProperties) {
        this.iotfsdbProperties = iotfsdbProperties;
    }

    @Bean
    public List<ToolCallback> toolCallbacks(McpTools mcpTools) {
        if (!iotfsdbProperties.getAi().isMcp()) {
            log.info("MCP tools are disabled");
            return List.of();
        }
        log.info("Registering MCP tools");
        return List.of(ToolCallbacks.from(mcpTools));
    }

}
