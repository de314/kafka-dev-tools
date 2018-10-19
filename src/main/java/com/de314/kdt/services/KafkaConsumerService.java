package com.de314.kdt.services;

import com.de314.kdt.kakfa.KDTConsumerGroup;
import com.de314.kdt.models.KDTConsumerConfig;
import com.de314.kdt.models.Page;

public interface KafkaConsumerService {

    Page<KDTConsumerGroup> findAll(int page, int size);

    KDTConsumerGroup findById(String id);

    KDTConsumerGroup getConsumer(KDTConsumerConfig config);

    KDTConsumerGroup dispose(String consumerId);
}
