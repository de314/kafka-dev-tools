package com.de314.kdt.controllers;

import com.de314.kdt.config.KafkaDevToolsConfig;
import com.de314.kdt.kakfa.InMemMessageQueue;
import com.de314.kdt.kakfa.KDTConsumerGroup;
import com.de314.kdt.kakfa.SeekingConsumerGroup;
import com.de314.kdt.models.ConsumerPeekResponse;
import com.de314.kdt.models.ConsumerResponse;
import com.de314.kdt.models.DeserializerInfoModel;
import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.models.MessageContainer;
import com.de314.kdt.models.Page;
import com.de314.kdt.models.RestException;
import com.de314.kdt.models.SupportedEnvironment;
import com.de314.kdt.services.DeserializerRegistryService;
import com.de314.kdt.services.KafkaConsumerService;
import com.de314.kdt.services.KafkaEnvironmentRegistryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import static com.de314.kdt.models.KDTConsumerConfig.KDTConsumerGroupType.SEEKING;
import static com.de314.kdt.models.KDTConsumerConfig.KDTConsumerGroupType.STREAMING;

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

    private KDTConsumerConfig getConfig(String kEnvId, String topic, String deserializerId, KDTConsumerConfig.KDTConsumerGroupType type) throws RestException {
        if (!consumersEnabled) {
            throw new RestException("Consumer Feature is not enabled.", HttpStatus.NOT_IMPLEMENTED);
        }
        SupportedEnvironment kafkaEnv = kafkaEnvironmentRegistryService.findById(kEnvId);
        if (kafkaEnv == null) {
            throw new RestException("Unknown Environment: " + kEnvId, HttpStatus.BAD_REQUEST);
        }
        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        if (des == null) {
            throw new RestException("Invalid deserializer id: " + deserializerId, HttpStatus.BAD_REQUEST);
        }

        return KDTConsumerConfig.builder()
                .type(type)
                .topic(topic)
                .kafkaEnvironment(kafkaEnv)
                .keyDeserializer(StringDeserializer.class.getName())
                .valueDeserializer(des)
                .build();
    }

    @GetMapping("/stream/{topic}")
    public ResponseEntity<ConsumerResponse> stream(
            @PathVariable("topic") String topic,
            @RequestParam("env") String kEnvId,
            @RequestParam("deserializerId") String deserializerId) {
        KDTConsumerConfig config = null;
        try {
            config = getConfig(kEnvId, topic, deserializerId, STREAMING);
        } catch (RestException e) {
            return ResponseEntity.status(e.getStatus())
                    .header("KDT-Error", e.getMessage())
                    .body(null);
        }

        KDTConsumerGroup consumer = kafkaConsumerService.getConsumer(config);
        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);

        Page<JsonNode> page = getPage(consumer, des);
        if (page == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("KDT-Error", "Error fetching results")
                    .body(null);
        }
        return ResponseEntity.ok(ConsumerResponse.builder()
                .consumerId(consumer.getConfig().getId())
                .page(page)
                .build());
    }

    @GetMapping("/seek/{topic}")
    public ResponseEntity<ConsumerResponse> seek(
            @PathVariable("topic") String topic,
            @RequestParam("env") String kEnvId,
            @RequestParam("deserializerId") String deserializerId) {
        KDTConsumerConfig config = null;
        try {
            config = getConfig(kEnvId, topic, deserializerId, SEEKING);
        } catch (RestException e) {
            return ResponseEntity.status(e.getStatus())
                    .header("KDT-Error", e.getMessage())
                    .body(null);
        }

        if (config.getType() != SEEKING) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Id does not correspond to a seeking consumer.")
                    .body(null);
        }

        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        KDTConsumerGroup consumer = kafkaConsumerService.getConsumer(config);
        consumer.poll(500);

        Page<JsonNode> page = getPage(consumer, des);
        if (page == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("KDT-Error", "Error fetching results")
                    .body(null);
        }
        return ResponseEntity.ok(ConsumerResponse.builder()
                .consumerId(consumer.getConfig().getId())
                .page(page)
                .build());
    }

    @GetMapping("/seek/{topic}/rewind")
    public ResponseEntity<ConsumerResponse> rewind(
            @PathVariable("topic") String topic,
            @RequestParam("env") String kEnvId,
            @RequestParam("deserializerId") String deserializerId) {
        KDTConsumerConfig config = null;
        try {
            config = getConfig(kEnvId, topic, deserializerId, SEEKING);
        } catch (RestException e) {
            return ResponseEntity.status(e.getStatus())
                    .header("KDT-Error", e.getMessage())
                    .body(null);
        }

        if (config.getType() != SEEKING) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Id does not correspond to a seeking consumer.")
                    .body(null);
        }

        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        KDTConsumerGroup consumer = kafkaConsumerService.getConsumer(config);
        consumer.rewind();

        Page<JsonNode> page = getPage(consumer, des);
        if (page == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("KDT-Error", "Error fetching results")
                    .body(null);
        }
        return ResponseEntity.ok(ConsumerResponse.builder()
                .consumerId(consumer.getConfig().getId())
                .page(page)
                .build());
    }

    @GetMapping("/seek/{topic}/{offset}")
    public ResponseEntity<ConsumerResponse> seek(
            @PathVariable("topic") String topic,
            @PathVariable("offset") long offset,
            @RequestParam("env") String kEnvId,
            @RequestParam("deserializerId") String deserializerId) {
        KDTConsumerConfig config;
        try {
            config = getConfig(kEnvId, topic, deserializerId, SEEKING);
        } catch (RestException e) {
            return ResponseEntity.status(e.getStatus())
                    .header("KDT-Error", e.getMessage())
                    .body(null);
        }

        if (config.getType() != SEEKING) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Id does not correspond to a seeking consumer.")
                    .body(null);
        }

        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        KDTConsumerGroup consumer = kafkaConsumerService.getConsumer(config);
        SeekingConsumerGroup seeker = (SeekingConsumerGroup) consumer;
        seeker.jumpTo(offset);

        Page<JsonNode> page = getPage(consumer, des);
        if (page == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("KDT-Error", "Error fetching results")
                    .body(null);
        }
        return ResponseEntity.ok(ConsumerResponse.builder()
                .consumerId(consumer.getConfig().getId())
                .page(page)
                .build());
    }

    @GetMapping("/peek/{topic}/{partition}/{offset}")
    public ResponseEntity<ConsumerPeekResponse> peek(
            @PathVariable("topic") String topic,
            @PathVariable("partition") int partition,
            @PathVariable("offset") long offset,
            @RequestParam("env") String kEnvId,
            @RequestParam("deserializerId") String deserializerId) {
        KDTConsumerConfig config = null;
        try {
            config = getConfig(kEnvId, topic, deserializerId, SEEKING);
        } catch (RestException e) {
            return ResponseEntity.status(e.getStatus())
                    .header("KDT-Error", e.getMessage())
                    .body(null);
        }

        if (config.getType() != SEEKING) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Id does not correspond to a seeking consumer.")
                    .body(null);
        }

        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        KDTConsumerGroup consumer = kafkaConsumerService.getConsumer(config);
        SeekingConsumerGroup seeker = (SeekingConsumerGroup) consumer;
        seeker.peek(partition, offset);
        MessageContainer peekResult = seeker.getQueue().getPeek();

        if (peekResult == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("KDT-Error", "No record found for partition@offset: " + partition + "@" + offset)
                    .body(null);
        }

        return ResponseEntity.ok(ConsumerPeekResponse.builder()
                .consumerId(consumer.getConfig().getId())
                .record(toNode(peekResult, des))
                .build());
    }

    private Page<JsonNode> getPage(KDTConsumerGroup consumer, DeserializerInfoModel des) {
        InMemMessageQueue queue = consumer.getQueue();

        Long since = -1L; //getSince(oSince, oWindow);
        Page<JsonNode> page = null;
        try {
            List<JsonNode> messages = queue.get(since).stream()
                    .map(mc -> toNode(mc, des))
                    .collect(Collectors.toList());
            page = new Page<>();
            page.setPage(0);
            page.setSize(messages.size());
            page.setTotalElements(queue.total());
            page.setContent(messages);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return page;
    }

    private JsonNode toNode(MessageContainer mc, DeserializerInfoModel des) {
        return mc == null ? NullNode.getInstance() : jsonObjectMapper.createObjectNode()
                .put("writeTime", mc.getWriteTime())
                .put("key", mc.getKey())
                .put("offset", mc.getOffset())
                .put("partition", mc.getPartition())
                .put("topic", mc.getTopic())
                .set("message", des.getPrepareOutputFunc().apply(mc.getMessage()));
    }

    protected long getSince(Optional<Long> oSince, Optional<Long> oWindow) {
        return oSince.isPresent() ? oSince.get() :
                oWindow.map(win -> System.currentTimeMillis() - win * 1000).orElse(-1L);
    }
}
