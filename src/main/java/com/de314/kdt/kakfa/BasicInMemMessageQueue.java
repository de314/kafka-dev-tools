package com.de314.kdt.kakfa;

import com.de314.kdt.models.MessageContainer;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by davidesposito on 7/1/16.
 */
@Slf4j
public class BasicInMemMessageQueue implements InMemMessageQueue {

    private final FixedSizeList<MessageContainer> messageQueue;
    private final AtomicLong total = new AtomicLong(0L);
    @Getter
    private long lastReadTime;
    @Getter
    private long lastMessageTime;
    private MessageContainer peek;

    public BasicInMemMessageQueue(int maxSize) {
        messageQueue = new FixedSizeList<>(maxSize);
        lastReadTime = System.currentTimeMillis();
    }

    protected boolean shouldPersist(ConsumerRecord<String, Object> record) {
        return true;
    }

    @Override
    public void handle(ConsumerRecord<String, Object> record) {
        if (!shouldPersist(record)) {
            log.debug("(DROPPING) received => {}, queued => {}",total.get() + 1, messageQueue.spine.size());
            return;
        }
        log.debug("PROCESSING receiving => {}, queued => {}",total.get() + 1, messageQueue.spine.size());
        total.incrementAndGet();
        long currTime = System.currentTimeMillis();
        lastMessageTime = currTime;
        this.messageQueue.add(MessageContainer.builder()
                .key(record.key())
                .message(record.value())
                .offset(record.offset())
                .partition(record.partition())
                .topic(record.topic())
                .writeTime(currTime)
                .build());
    }

    public void setPeek(ConsumerRecord<String, Object> record) {
        long currTime = System.currentTimeMillis();
        lastMessageTime = currTime;
        this.peek = MessageContainer.builder()
                .key(record.key())
                .message(record.value())
                .offset(record.offset())
                .partition(record.partition())
                .topic(record.topic())
                .writeTime(currTime)
                .build();
    }

    public MessageContainer getPeek() {
        return peek;
    }

    public List<MessageContainer> get(Long since) {
        lastReadTime = System.currentTimeMillis();
        return messageQueue.stream()
                .filter(c -> isValidDate(since, c.getWriteTime()))
                .collect(Collectors.toList());
    }

    public int count(Long since) {
        return (int)messageQueue.stream()
                .filter(c -> isValidDate(since, c.getWriteTime()))
                .count();
    }

    public long total() {
        return total.get();
    }

    public long getQueueSize() {
        return messageQueue.maxSize;
    }

    public void clear() {
        total.set(0L);
        messageQueue.clear();
    }

    protected boolean isValidDate(Long since, Long writeTime) {
        return since < 0 || writeTime > since;
    }

    protected static class FixedSizeList<E> {

        private final LinkedList<E> spine;
        private final int maxSize;

        public FixedSizeList(int maxSize) {
            spine = Lists.newLinkedList();
            this.maxSize = Math.min(Math.max(maxSize, 1), 2000);
        }

        public synchronized void add(E ele) {
            if (spine.size() >= maxSize) {
                spine.removeFirst();
            }
            spine.add(ele);
        }

        public void clear() {
            spine.clear();
        }

        public synchronized Stream<E> stream() {
            return spine.stream();
        }
    }
}
