package com.de314.kdt.kakfa;

import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.utils.Opt;
import com.de314.kdt.utils.Pair;
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
public class SeekingConsumerGroup extends AbstractConsumerGroup {

    public SeekingConsumerGroup(KDTConsumerConfig config) {
        super(config);
    }

    @Override
    public void shutdown() {
        getShutdownRequest().set(true);
        try {
            consumer.wakeup();
            consumer.close();
        } catch (Exception e) {
            log.error("failed shutting down", e);
        }
        TopicErrorMap.clear(config.getTopic(), config.getValueDeserializer().getId());
    }

    public void peek(int partition, long offset) {
        getQueue().clear();
        _jumpTo(offset);
        ConsumerRecords<String, Object> records = consumer.poll(0);
        for (ConsumerRecord<String, Object> record : records) {
            if (record.partition() == partition && record.offset() == offset) {
                getQueue().setPeek(record);
            }
            getQueue().handle(record);
        }
    }

    public void jumpTo(long offset) {
        getQueue().clear();
        _jumpTo(offset);
        poll();
    }

    private void _jumpTo(long offset) {
        Set<TopicPartition> topicPartitions = consumer.assignment();
        topicPartitions.stream()
                .map(tp -> Pair.of(tp, consumer.position(tp)))
                .forEach(pair -> {
                    TopicPartition tp = pair.getLeft();
                    consumer.seek(tp, offset);
                });
    }
}
