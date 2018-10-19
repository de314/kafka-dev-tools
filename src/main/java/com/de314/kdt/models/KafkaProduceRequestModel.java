package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/1/16.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProduceRequestModel {

    private String topic;
    private String key;
    private String rawMessage;

    private KafkaProduceMessageMetaModel meta;

}
