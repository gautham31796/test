package com.example.integration.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.messaging.MessageChannel;

import java.io.File;
import java.time.Duration;

@Configuration
@EnableIntegration
@IntegrationComponentScan
public class IntegrationConfig {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationConfig.class);

    @Bean
    public MessageChannel inputChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow fileIntegrationFlow() {
        return IntegrationFlow
            .from(Files.inboundAdapter(new File("input"))
                        .preventDuplicates(true)
                        .patternFilter("*.json"),
                  e -> e.poller(Pollers.fixedRate(Duration.ofMinutes(2))))
            .handle(File.class, (file, headers) -> {
                logger.info("Picked up file: {}", file.getName());
                return file;
            })
            .transform(File.class, file -> {
                logger.info("Starting transformation of file: {}", file.getName());
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode root = objectMapper.readTree(file);

                    if (root.isObject()) {
                        ObjectNode rootObj = (ObjectNode) root;
                        JsonNode testData = rootObj.path("testData");

                        if (testData.isObject()) {
                            ObjectNode newTestData = objectMapper.createObjectNode();
                            testData.fieldNames().forEachRemaining(key ->
                                newTestData.set(key.toLowerCase(), testData.get(key))
                            );

                            rootObj.set("testData", newTestData);
                            String result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootObj);
                            return result;
                        }
                    }

                    logger.warn("Root or testData was not an object node.");
                    return objectMapper.writeValueAsString(root);
                } catch (Exception e) {
                    logger.error("Failed to transform file: {}", file.getName(), e);
                    throw new RuntimeException("Failed to transform JSON", e);
                }
            })
            .handle((payload, headers) -> {
                logger.info("Final transformed JSON:\n{}", payload);
                return null;
            })
            .get();
    }
}
