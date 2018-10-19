package com.de314.kdt.services;

import com.de314.kdt.models.SupportedEnvironment;

import java.util.List;

/**
 * Created by davidesposito on 8/28/16.
 */
public interface KafkaTopicService {

    List<String> getAll(SupportedEnvironment supportedEnvironment);
}
