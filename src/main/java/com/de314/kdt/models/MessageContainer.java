package com.de314.kdt.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MessageContainer {

    private final long writeTime;
    private final String key;
    private final String topic;
    private final int partition;
    private final long offset;
    private final Object message;
}
