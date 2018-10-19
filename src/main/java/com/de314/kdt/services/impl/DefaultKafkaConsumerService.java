package com.de314.kdt.services.impl;

import com.de314.kdt.kakfa.BasicInMemMessageQueue;
import com.de314.kdt.kakfa.BasicKafkaConsumerGroup;
import com.de314.kdt.kakfa.KDTConsumerGroup;
import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.models.Page;
import com.de314.kdt.services.KafkaConsumerService;
import com.google.common.collect.Maps;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DefaultKafkaConsumerService implements KafkaConsumerService {

    private final Map<String, KDTConsumerGroup> repo;

    public DefaultKafkaConsumerService() {
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
            BasicKafkaConsumerGroup consumer = new BasicKafkaConsumerGroup(config);
            repo.put(key, consumer);
            consumer.run();
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
}
