package com.de314.kdt.kakfa;

import com.de314.kdt.models.KDTConsumerConfig;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface KDTConsumerGroup {

    KDTConsumerConfig getConfig();

    InMemMessageQueue getQueue();

    String getClientId();

    String getGroupId();

    boolean isDead();

    void shutdown();
}
