package com.de314.kdt.kakfa;

import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.utils.Opt;
import com.de314.kdt.utils.Pair;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by davidesposito on 7/20/16.
 */
@Slf4j
@Getter(AccessLevel.PROTECTED)
public abstract class AbstractConsumerGroup implements KDTConsumerGroup {

    protected final KafkaConsumer<String, Object> consumer;
    protected final KDTConsumerConfig config;
    protected final String clientId;
    private final String groupId;
    private final AtomicBoolean shutdownRequest;

    public AbstractConsumerGroup(KDTConsumerConfig config) {
        assert(config != null);
        assert(config.getKeyDeserializer() != null);
        assert(config.getValueDeserializer() != null);

        this.config = config;
        this.clientId = "kdt-" + UUID.randomUUID().toString();
        this.groupId = this.clientId;

        this.consumer = getConsumer();
        this.shutdownRequest = new AtomicBoolean(false);

        consumer.subscribe(Arrays.asList(config.getTopic()));
        poll();
        rewindByMultiple(1);
    }

    protected KafkaConsumer<String, Object> getConsumer() {
        log.info("Initializing Consumer: {}", config);

        final Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getKafkaEnvironment().getKafkaHost());
        properties.put("schema.registry.url", config.getKafkaEnvironment().getSchemaUrl());

        properties.put("client.id", getClientId());
        properties.put("group.id", getGroupId());

        properties.put("key.deserializer", config.getKeyDeserializer());
        properties.put("value.deserializer", config.getValueDeserializer().getClassName());

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(config.getTopic()));

        return consumer;
    }

    @Override
    public KDTConsumerConfig getConfig() {
        return config;
    }

    @Override
    public InMemMessageQueue getQueue() {
        return this.config.getHandler();
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public boolean isDead() {
        return shutdownRequest.get();
    }

    public void poll() {
        poll(0L);
    }

    public void poll(long pollTime) {
        ConsumerRecords<String, Object> records = consumer.poll(pollTime);
        for (ConsumerRecord<String, Object> record : records) {
            config.getHandler().handle(record);
        }
    }

    private void rewindByMultiple(int multiple) {
        getQueue().clear();
        Set<TopicPartition> topicPartitions = consumer.assignment();
        int perPartitionRewind = 100 / topicPartitions.size() * multiple;
        topicPartitions.stream()
                .map(tp -> Pair.of(tp, consumer.position(tp)))
                .forEach(pair -> {
                    TopicPartition tp = pair.getLeft();
                    Opt.of(pair.getRight())
                            .map(offset -> {
                                long newOffset = Math.max(0, offset - perPartitionRewind);
                                log.info("Seeking {} to {} (per partition rewind {}) from {}",
                                        tp.toString(), newOffset, perPartitionRewind, offset);
                                return newOffset;
                            })
                            .ifPresent(newOffset -> consumer.seek(tp, newOffset))
                            .notPresent(() -> log.error("Could not calculate new offset for {}", tp.toString()));
                });
        poll(1000);
    }

    public void rewind() {
        rewindByMultiple(2);
    }
}
