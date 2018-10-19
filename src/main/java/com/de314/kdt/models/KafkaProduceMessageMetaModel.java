package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/1/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProduceMessageMetaModel {

    private String environmentId;
    private String serializerId;
    private String schema;
    private String rawSchema;

    public String getId() {
        return String.format("%s@@%s", environmentId, serializerId);
    }
}
