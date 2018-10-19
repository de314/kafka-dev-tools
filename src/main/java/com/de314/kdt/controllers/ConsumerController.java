package com.de314.kdt.controllers;

import com.de314.kdt.config.KafkaDevToolsConfig;
import com.de314.kdt.kakfa.InMemMessageQueue;
import com.de314.kdt.kakfa.KDTConsumerGroup;
import com.de314.kdt.models.ConsumerResponse;
import com.de314.kdt.models.DeserializerInfoModel;
import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.models.MessageContainer;
import com.de314.kdt.models.Page;
import com.de314.kdt.models.SupportedEnvironment;
import com.de314.kdt.services.DeserializerRegistryService;
import com.de314.kdt.services.KafkaConsumerService;
import com.de314.kdt.services.KafkaEnvironmentRegistryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/consumers")
public class ConsumerController {

    private final DeserializerRegistryService deserializerRegistryService;
    private final KafkaEnvironmentRegistryService kafkaEnvironmentRegistryService;
    private final KafkaConsumerService kafkaConsumerService;
    private final ObjectMapper jsonObjectMapper;

    private final boolean consumersEnabled;

    public ConsumerController(DeserializerRegistryService deserializerRegistryService,
            KafkaEnvironmentRegistryService kafkaEnvironmentRegistryService,
            KafkaConsumerService kafkaConsumerService,
            ObjectMapper jsonObjectMapper,
            KafkaDevToolsConfig kafkaDevToolsConfig) {
        this.deserializerRegistryService = deserializerRegistryService;
        this.kafkaEnvironmentRegistryService = kafkaEnvironmentRegistryService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.jsonObjectMapper = jsonObjectMapper;

        consumersEnabled = kafkaDevToolsConfig.getConsumer().isEnabled();
    }

    @GetMapping("/poll/{topic}")
    public ResponseEntity<ConsumerResponse> readKafka(
            @PathVariable("topic") String topic,
            @RequestParam("env") String kEnvId,
            @RequestParam("deserializerId") String deserializerId) {
        if (!consumersEnabled) {
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
                    .header("KDT-Error", "Consumer Feature is not enabled.")
                    .body(null);
        }
        SupportedEnvironment kafkaEnv = kafkaEnvironmentRegistryService.findById(kEnvId);
        if (kafkaEnv == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("KDT-Error", "Unknown Environment: " + kEnvId)
                    .body(null);
        }
        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        if (des == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("KDT-Error", "Invalid deserializer id: " + deserializerId)
                    .body(null);
        }

        KDTConsumerConfig config = KDTConsumerConfig.builder()
                .topic(topic)
                .kafkaEnvironment(kafkaEnv)
                .keyDeserializer(StringDeserializer.class.getName())
                .valueDeserializer(des)
                .build();

        KDTConsumerGroup consumer = kafkaConsumerService.getConsumer(config);

        InMemMessageQueue queue = consumer.getQueue();

        Long since = -1L; //getSince(oSince, oWindow);
        Page<JsonNode> page;
        try {
            List<JsonNode> messages = queue.get(since).stream()
                    .map(m -> (MessageContainer) m)
                    .map(mc -> jsonObjectMapper.createObjectNode()
                            .put("writeTime", mc.getWriteTime())
                            .put("key", mc.getKey())
                            .put("offset", mc.getOffset())
                            .put("partition", mc.getPartition())
                            .put("topic", mc.getTopic())
                            .set("message", des.getPrepareOutputFunc().apply(mc.getMessage())))
                    .collect(Collectors.toList());
            page = new Page<>();
            page.setPage(0);
            page.setSize(messages.size());
            page.setTotalElements(queue.total());
            page.setContent(messages);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("KDT-Error", e.getMessage())
                    .body(null);
        }
        return ResponseEntity.ok(ConsumerResponse.builder()
                .consumerId(consumer.getConfig().getId())
                .page(page)
                .build());
    }

    protected long getSince(Optional<Long> oSince, Optional<Long> oWindow) {
        return oSince.isPresent() ? oSince.get() :
                oWindow.map(win -> System.currentTimeMillis() - win * 1000).orElse(-1L);
    }
}
