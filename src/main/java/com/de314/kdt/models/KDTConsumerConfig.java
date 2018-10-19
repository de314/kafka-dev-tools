package com.de314.kdt.models;

import com.de314.kdt.kakfa.InMemMessageQueue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Created by davidesposito on 7/19/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KDTConsumerConfig {

    public enum KDTConsumerGroupType {
        STREAMING,
        SEEKING
    }

    private KDTConsumerGroupType type;
    private String topic;
    private SupportedEnvironment kafkaEnvironment;
    private String keyDeserializer;
    private DeserializerInfoModel valueDeserializer;

    public String getId() {
        // TODO: add filter hash
        String filterHash = "not_implemented";
        return String.format("%s@@%s@@%s@@%s@@%s",
                type, kafkaEnvironment.getId(), topic, valueDeserializer.getId(), filterHash);
    }

    @JsonIgnore
    private InMemMessageQueue handler;
}
