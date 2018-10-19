package com.de314.kdt.kakfa;

import com.de314.kdt.models.KDTConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by davidesposito on 7/20/16.
 */
@Slf4j
public class StreamingConsumerGroup extends AbstractConsumerGroup {

    public StreamingConsumerGroup(KDTConsumerConfig config) {
        super(config);
    }

    @Override
    public void shutdown() {
        getShutdownRequest().set(true);
        TopicErrorMap.clear(config.getTopic(), config.getValueDeserializer().getId());
    }

    public KDTConsumerGroup start() {
        final String topic = config.getTopic();
        final String deserializerId = config.getValueDeserializer().getId();
        AtomicBoolean shutdownRequest = getShutdownRequest();
        final KDTConsumerGroup that = this;
        super.poll(500);
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
        return this;
    }
}
