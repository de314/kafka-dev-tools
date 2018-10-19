package com.de314.kdt.models;

import com.de314.kdt.kakfa.InMemMessageQueue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Base64;

/**
 * Created by davidesposito on 7/19/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KDTConsumerConfig {

    public static final Base64.Encoder ENCODER = Base64.getEncoder();

    public enum KDTConsumerGroupType {
        STREAMING,
        SEEKING
    }

    private KDTConsumerGroupType type;
    private String topic;
    private SupportedEnvironment kafkaEnvironment;
    private String keyDeserializer;
    private DeserializerInfoModel valueDeserializer;
    private String filter;

    public String getId() {
        // TODO: add filter hash
        String filterHash = filter == null ? "" : ENCODER.encodeToString(filter.getBytes());
        return String.format("%s@@%s@@%s@@%s@@%s",
                type, kafkaEnvironment.getId(), topic, valueDeserializer.getId(), filterHash);
    }

    @JsonIgnore
    private InMemMessageQueue handler;
}
