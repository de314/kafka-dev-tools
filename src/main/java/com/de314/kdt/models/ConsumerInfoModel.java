package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/20/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerInfoModel {

    private String id;
    private String topic;
    private long lastUsedTime;
    private long lastMessageTime;
    private String consumerGroupId;
    private long queueSize;
    private long total;
    private String deserializerName;
    private String deserializerId;
    private long errors;
    private SupportedEnvironment environment;
}
