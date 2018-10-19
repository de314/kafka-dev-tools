package com.de314.kdt.controllers;

import com.de314.kdt.kakfa.InMemMessageQueue;
import com.de314.kdt.kakfa.TopicErrorMap;
import com.de314.kdt.models.ConsumerInfoModel;
import com.de314.kdt.models.DeserializerInfoModel;
import com.de314.kdt.models.Page;
import com.de314.kdt.models.SerializerInfoModel;
import com.de314.kdt.models.SupportedEnvironment;
import com.de314.kdt.services.DeserializerRegistryService;
import com.de314.kdt.services.KafkaConsumerService;
import com.de314.kdt.services.KafkaEnvironmentRegistryService;
import com.de314.kdt.services.KafkaTopicService;
import com.de314.kdt.services.SerializerRegistryService;
import com.de314.kdt.utils.Opt;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/manager")
public class ManagerController {

    private final KafkaEnvironmentRegistryService kafkaEnvironmentRegistryService;
    private final SerializerRegistryService serializerRegistryService;
    private final DeserializerRegistryService deserializerRegistryService;
    private final KafkaTopicService kafkaTopicService;
    private final KafkaConsumerService kafkaConsumerService;

    public ManagerController(KafkaEnvironmentRegistryService kafkaEnvironmentRegistryService,
            SerializerRegistryService serializerRegistryService,
            DeserializerRegistryService deserializerRegistryService,
            KafkaTopicService kafkaTopicService,
            KafkaConsumerService kafkaConsumerService) {
        this.kafkaEnvironmentRegistryService = kafkaEnvironmentRegistryService;
        this.serializerRegistryService = serializerRegistryService;
        this.deserializerRegistryService = deserializerRegistryService;
        this.kafkaTopicService = kafkaTopicService;
        this.kafkaConsumerService = kafkaConsumerService;
    }

    @GetMapping("/environments")
    public ResponseEntity<Page<SupportedEnvironment>> environments() {
        return ResponseEntity.ok(kafkaEnvironmentRegistryService.findAll());
    }

    @GetMapping("/serializers")
    public ResponseEntity<Page<SerializerInfoModel>> serializers() {
        return ResponseEntity.ok(serializerRegistryService.findAll());
    }

    @GetMapping("/deserializers")
    public ResponseEntity<Page<DeserializerInfoModel>> deserializers() {
        return ResponseEntity.ok(deserializerRegistryService.findAll());
    }

    @GetMapping("/topics")
    public ResponseEntity<List<String>> topics(@RequestParam("env") String kEnvId) {
        SupportedEnvironment env = kafkaEnvironmentRegistryService.findById(kEnvId);
        if (env == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("KDT-Error", "Unknown environment: " + kEnvId)
                    .body(null);
        }
        return ResponseEntity.ok(kafkaTopicService.getAll(env));
    }

    @GetMapping("/consumers")
    public ResponseEntity<Page<ConsumerInfoModel>> getAllConsumers() {
        Page<ConsumerInfoModel> consumers = new Page<>();
        List<ConsumerInfoModel> content = kafkaConsumerService.findAll(0, 100).getContent().stream()
                .map(consumer -> {
                    InMemMessageQueue queue = consumer.getQueue();
                    return ConsumerInfoModel.builder()
                            .id(consumer.getConfig().getId())
                            .consumerGroupId(consumer.getGroupId())
                            .deserializerName(consumer.getConfig().getValueDeserializer().getName())
                            .deserializerId(consumer.getConfig().getValueDeserializer().getId())
                            .lastMessageTime(queue.getLastMessageTime())
                            .lastUsedTime(queue.getLastReadTime())
                            .queueSize(queue.getQueueSize())
                            .topic(consumer.getConfig().getTopic())
                            .total(queue.total())
                            .environment(consumer.getConfig().getKafkaEnvironment())
                            .errors(TopicErrorMap.peek(consumer.getConfig().getTopic(), consumer.getConfig().getValueDeserializer().getId()))
                            .build();
                })
                .collect(Collectors.toList());
        consumers.setContent(content);
        consumers.setPage(0);
        consumers.setTotalElements(content.size());
        consumers.setTotalElements(content.size());
        return ResponseEntity.ok(consumers);
    }

    @DeleteMapping("/consumers/{consumerId}")
    public ResponseEntity<Boolean> kill(@PathVariable("consumerId") String id) {
        if (!Opt.of(kafkaConsumerService.dispose(id)).isPresent()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("KDT-Error", "Consumer Not Found")
                    .body(null);
        }
        return ResponseEntity.ok(true);
    }

    @DeleteMapping("/consumers/{consumerId}/truncate")
    public ResponseEntity<Boolean> truncate(@PathVariable("consumerId") String id) {
        if (!Opt.of(kafkaConsumerService.findById(id))
                .ifPresent(container -> container.getQueue().clear())
                .isPresent()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("KDT-Error", "Consumer Not Found")
                    .body(null);
        }
        return ResponseEntity.ok(true);
    }
}
