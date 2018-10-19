package com.de314.kdt.services.impl;

import com.de314.kdt.config.KafkaDevToolsConfig;
import com.de314.kdt.models.SupportedEnvironment;
import com.de314.kdt.services.KafkaEnvironmentRegistryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

/**
 * Created by davidesposito on 7/21/16.
 */
@Service
public class DefaultKafkaEnvironmentRegistryService extends SimpleRegistryService<SupportedEnvironment>
        implements KafkaEnvironmentRegistryService, CommandLineRunner {

    @Autowired private KafkaDevToolsConfig config;

    @Override
    public void run(String... args) {
        config.getEnvironments().getSupportedEnvironments().forEach(this::register);
    }
}
