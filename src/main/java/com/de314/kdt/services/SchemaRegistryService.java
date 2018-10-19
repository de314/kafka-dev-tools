package com.de314.kdt.services;

import com.de314.kdt.models.SchemaInfoModel;
import com.de314.kdt.models.SchemaRegistryRestException;
import com.de314.kdt.models.SchemaVersionModel;

import java.util.List;

/**
 * Created by davidesposito on 7/18/16.
 */
public interface SchemaRegistryService {

    List<String> findAll(String oUrl, boolean skipCache) throws SchemaRegistryRestException;

    List<String> guessAllTopics(String oUrl, boolean skipCache) throws SchemaRegistryRestException;

    SchemaInfoModel getInfo(String name, String oUrl, boolean skipCache) throws SchemaRegistryRestException;

    SchemaVersionModel getVersion(String name, int version, String oUrl, boolean skipCache) throws SchemaRegistryRestException;
}
