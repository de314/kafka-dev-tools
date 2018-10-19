package com.de314.kdt.services.impl;

import com.de314.kdt.config.KafkaDevToolsConfig;
import com.de314.kdt.kakfa.BasicInMemMessageQueue;
import com.de314.kdt.kakfa.SeekingConsumerGroup;
import com.de314.kdt.kakfa.StreamingConsumerGroup;
import com.de314.kdt.kakfa.KDTConsumerGroup;
import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.models.Page;
import com.de314.kdt.services.KafkaConsumerService;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DefaultKafkaConsumerService implements KafkaConsumerService {

    private final long idleThreshold;
    private final Map<String, KDTConsumerGroup> repo;

    public DefaultKafkaConsumerService(
            KafkaDevToolsConfig kafkaDevToolsConfig
    ) {
        this.idleThreshold = kafkaDevToolsConfig.getConsumer().getIdleThreshold();
        this.repo = Maps.newConcurrentMap();
    }

    @Override
    public Page<KDTConsumerGroup> findAll(int page, int size) {
        return Page.<KDTConsumerGroup>builder()
                .content(
                        repo.values().stream().skip(page * size).limit(size).collect(Collectors.toList())
                )
                .page(page)
                .size(size)
                .totalElements(repo.size())
                .build();
    }

    @Override
    public KDTConsumerGroup findById(String id) {
        return repo.get(id);
    }

    @Override
    public KDTConsumerGroup getConsumer(KDTConsumerConfig config) {
        String key = config.getId();
        if (!repo.containsKey(key)) {
            if (config.getHandler() == null) {
                config.setHandler(new BasicInMemMessageQueue(100)); // TODO: configurable
            }
            KDTConsumerGroup consumer;
            if (config.getType() == null || config.getType() == KDTConsumerConfig.KDTConsumerGroupType.STREAMING) {
                consumer = new StreamingConsumerGroup(config).start();
            } else {
                consumer = new SeekingConsumerGroup(config);
            }
            repo.put(key, consumer);
        }
        return repo.get(key);
    }

    @Override
    public KDTConsumerGroup dispose(String consumerId) {
        KDTConsumerGroup consumer = repo.remove(consumerId);
        if (consumer != null) {
            consumer.shutdown();
        }
        return consumer;
    }

    @Scheduled(fixedRate = 300000)
    public void cleanUp() {
        Set<String> keys = repo.keySet();
        long now = System.currentTimeMillis();
        int count = 0;
        for (String key : keys) {
            long lastReadTime = repo.get(key).getQueue().getLastReadTime();
            long readLatency = now - lastReadTime;
            if (readLatency > idleThreshold) {
                this.dispose(key);
                count++;
            }
        }
        log.info("Cleaning up " + count + " idle consumers");
    }
}
