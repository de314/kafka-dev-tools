package com.de314.kdt.kakfa;

import com.de314.kdt.models.MessageContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface InMemMessageQueue {

    void handle(ConsumerRecord<String, Object> record);

    List<MessageContainer> get(Long since);

    MessageContainer getPeek();

    void setPeek(ConsumerRecord<String, Object> record);

    int count(Long since);

    long total();

    long getQueueSize();

    long getLastMessageTime();

    long getLastReadTime();

    void clear();
}
