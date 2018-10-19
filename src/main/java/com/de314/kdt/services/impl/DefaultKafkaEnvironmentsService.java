package com.de314.kdt.services.impl;

import com.de314.kdt.config.KadminConfig;
import com.de314.kdt.services.KafkaEnvironmentsService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class DefaultKafkaEnvironmentsService implements KafkaEnvironmentsService {

    private final KadminConfig kadminConfig;
    private final List<String> supportedKafkaHosts;
    private final List<String> getSupportedSchemaUrls;

    public DefaultKafkaEnvironmentsService(KadminConfig kadminConfig) {
        this.kadminConfig = kadminConfig;
        this.supportedKafkaHosts = kadminConfig.getEnvironments().getSupportedEnvironments().stream()
                .map(supportedEnvironment ->  supportedEnvironment.getKafkaHost())
                .collect(Collectors.toList());
        this.getSupportedSchemaUrls = kadminConfig.getEnvironments().getSupportedEnvironments().stream()
                .map(supportedEnvironment ->  supportedEnvironment.getSchemaUrl())
                .collect(Collectors.toList());
    }

    public String getCustomKafkaHost(String url) {
        return getCustomUrl(url, supportedKafkaHosts);
    }

    @Override
    public String getCustomSchemaUrl(String url) {
        return getCustomUrl(url, getSupportedSchemaUrls);
    }

    private String getCustomUrl(String url, List<String> supported) {
        return Optional.ofNullable(url)
                .filter(u -> kadminConfig.getEnvironments().isCustomUrlsEnabled() || supported.contains(u))
                .orElse(supported.get(0));
    }
}
