package com.de314.kdt.kakfa;

import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.utils.Opt;
import com.de314.kdt.utils.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by davidesposito on 7/20/16.
 */
@Slf4j
public class BasicKafkaConsumerGroup implements KDTConsumerGroup {

    private final KafkaConsumer<String, Object> consumer;
    private final KDTConsumerConfig config;
    private final String clientId;
    private final String groupId;
    private final AtomicBoolean shutdownRequest;

    public BasicKafkaConsumerGroup(KDTConsumerConfig config) {
        assert(config != null);
        assert(config.getKeyDeserializer() != null);
        assert(config.getValueDeserializer() != null);

        this.config = config;
        this.clientId = "kdt-" + UUID.randomUUID().toString();
        this.groupId = this.clientId;

        this.consumer = getConsumer();
        this.shutdownRequest = new AtomicBoolean(false);

        init();
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

        return new KafkaConsumer<>(properties);
    }

    protected void init() {
        consumer.subscribe(Arrays.asList(config.getTopic()));
        poll(0L);
        setInitialOffset();
    }

    private void setInitialOffset() {
        Set<TopicPartition> topicPartitions = consumer.assignment();
        int perPartitionRewind = 100 / topicPartitions.size();
        topicPartitions.stream()
                .map(tp -> Pair.of(tp, consumer.position(tp)))
                .forEach(pair -> {
                    TopicPartition tp = pair.getLeft();
                    Opt.of(pair.getRight())
                            .map(offset -> {
                                long newOffset = Math.max(0, offset - perPartitionRewind);
                                log.info("Seeking {} to {} (per partiion rewind {}) from {}",
                                        tp.toString(), newOffset, perPartitionRewind, offset);
                                return newOffset;
                            })
                            .ifPresent(newOffset -> consumer.seek(tp, newOffset))
                            .notPresent(() -> log.error("Could not calculate new offset for {}", tp.toString()));
                });
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

    @Override
    public void shutdown() {
        shutdownRequest.set(true);
        TopicErrorMap.clear(config.getTopic(), config.getValueDeserializer().getId());
    }

    private void poll() {
        poll(0L);
    }

    private void poll(long pollTime) {
        ConsumerRecords<String, Object> records = consumer.poll(pollTime);
        for (ConsumerRecord<String, Object> record : records) {
            config.getHandler().handle(record);
        }
    }

    public void run() {
        final String topic = config.getTopic();
        final String deserializerId = config.getValueDeserializer().getId();
        AtomicBoolean shutdownRequest = this.shutdownRequest;
        final KDTConsumerGroup that = this;
        this.poll(500);
        new Thread(() -> {
            try {
                while (!shutdownRequest.get()) {
                    if (TopicErrorMap.shouldKill(topic, deserializerId)) {
                        that.shutdown();
                        break;
                    }
                    poll();
                    try {
                        Thread.sleep(500); // TODO: configure
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } finally {
                consumer.wakeup();
                consumer.close();
            }
        }).start();
    }
}
