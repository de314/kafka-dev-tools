package com.de314.kdt.services.impl;

import avro.shaded.com.google.common.collect.Maps;
import com.de314.kdt.models.SupportedEnvironment;
import com.de314.kdt.services.KafkaTopicService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 8/28/16.
 */
@Service
public class DefaultKafkaTopicService implements KafkaTopicService {

    private final Cache<String, Map<String, List<PartitionInfo>>> partitionCache;

    public DefaultKafkaTopicService() {
        this.partitionCache = CacheBuilder.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public List<String> getAll(SupportedEnvironment supportedEnvironment) {
        return Optional.ofNullable(getPartitionMap(supportedEnvironment))
                .orElse(Maps.newHashMap())
                .keySet().stream()
                .collect(Collectors.toList());
    }

    protected Map<String, List<PartitionInfo>> getPartitionMap(SupportedEnvironment supportedEnvironment) {
        Map<String, List<PartitionInfo>> partitionMap = partitionCache.getIfPresent(supportedEnvironment.getKafkaHost());
        if (partitionMap == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", supportedEnvironment.getKafkaHost());
            props.put("group.id", "kafmin-topic-registry");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("exclude.internal.topics", true);

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            Map<String, List<PartitionInfo>> temp = consumer.listTopics();
            partitionMap = Collections.unmodifiableMap(temp);
            partitionCache.put(supportedEnvironment.getKafkaHost(), partitionMap);
            consumer.close();
        }
        return partitionMap;
    }
}
