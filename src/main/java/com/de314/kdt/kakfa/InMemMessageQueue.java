package com.de314.kdt.kakfa;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface InMemMessageQueue {

    void handle(ConsumerRecord<String, Object> record);

    List<Object> get(Long since);

    int count(Long since);

    long total();

    long getQueueSize();

    long getLastMessageTime();

    long getLastReadTime();

    void clear();
}
