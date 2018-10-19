package com.de314.kdt.config;

import com.de314.kdt.models.SupportedEnvironment;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by davidesposito on 6/14/17.
 */
@Data
@Component
@NoArgsConstructor
@ConfigurationProperties(prefix="kadmin", ignoreInvalidFields = true)
public class KadminConfig {

    private KafkaProducerConfig producer;
    private KafkaProducerConfig consumer;
    private KafkaEnvironmentsConfig environments;

    @Data
    public static class KafkaEnvironmentsConfig {
        private boolean customUrlsEnabled;
        private List<SupportedEnvironment> supportedEnvironments = Lists.newArrayList();
    }

    @Data
    public static class KafkaProducerConfig {
        private boolean enabled;
    }

    @Data
    public static class KafkaConsumerConfig {
        private boolean enabled;
    }
}
