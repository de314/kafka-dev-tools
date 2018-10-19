package com.de314.kdt.services.impl;

import com.de314.kdt.models.SchemaInfoModel;
import com.de314.kdt.models.SchemaRegistryRestException;
import com.de314.kdt.models.SchemaVersionModel;
import com.de314.kdt.services.KafkaEnvironmentsService;
import com.de314.kdt.services.SchemaRegistryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by davidesposito on 7/18/16.
 */
@Slf4j
@Service
public class DefaultSchemaRegistryService implements SchemaRegistryService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String schemaRegistryUrl;

    private final RestTemplate restTemplate;
    private final KafkaEnvironmentsService kafkaEnvironmentsService;

    private final Cache<String, List<String>> schemasCache;
    private final Cache<String, SchemaInfoModel> schemaInfoCache;
    private final Cache<String, SchemaVersionModel> schemaVersionCache;

    @Autowired
    public DefaultSchemaRegistryService(
            @Value("${schema.registry.url:http://localhost:8081}")
                    String schemaRegistryUrl,
            RestTemplate restTemplate,
            KafkaEnvironmentsService kafkaEnvironmentsService) {
        this.restTemplate = restTemplate;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.kafkaEnvironmentsService = kafkaEnvironmentsService;

        this.schemasCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();
        this.schemaInfoCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .maximumSize(100)
                .build();
        this.schemaVersionCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .maximumSize(100)
                .build();
    }

    @Override
    public List<String> findAll(String url, boolean skipCache) throws SchemaRegistryRestException {
       String  tempUrl = String.format("%s/subjects",
                Optional.ofNullable(kafkaEnvironmentsService.getCustomSchemaUrl(url)).orElse(this.schemaRegistryUrl)
        );
        if (schemasCache.getIfPresent(tempUrl) == null || skipCache) {
            NodeConverter<List<String>> c = (node) -> {
                if (node.isArray()) {
                    ArrayNode arr = (ArrayNode) node;
                    return StreamSupport.stream(arr.spliterator(), false)
                            .map(n -> n.asText())
                            .sorted()
                            .collect(Collectors.toList());
                }
                return null;
            };
            schemasCache.put(tempUrl, proxyResponse(tempUrl, c, null));
        } else {
            log.debug("Hit schema cache for: {}", tempUrl);
        }
        return schemasCache.getIfPresent(tempUrl);
    }

    @Override
    public List<String> guessAllTopics(String url, boolean skipCache) throws SchemaRegistryRestException {
        return findAll(url, skipCache).stream()
                .map(schemaName -> schemaName.replaceAll("-value", ""))
                .collect(Collectors.toList());
    }

    @Override
    public SchemaInfoModel getInfo(String name, String url, boolean skipCache) throws SchemaRegistryRestException {
        String tempUrl = String.format("%s/subjects/%s/versions",
                Optional.ofNullable(kafkaEnvironmentsService.getCustomSchemaUrl(url)).orElse(this.schemaRegistryUrl),
                name
        );
        if (schemaInfoCache.getIfPresent(tempUrl) == null || skipCache) {
            NodeConverter<List<Integer>> c = (node) -> {
                if (node.isArray()) {
                    ArrayNode arr = (ArrayNode) node;
                    return StreamSupport.stream(arr.spliterator(), false)
                            .map(n -> n.asInt())
                            .collect(Collectors.toList());
                }
                return null;
            };
            List<Integer> versions = proxyResponse(tempUrl, c, null);
            SchemaVersionModel currSchema = getVersion(name, versions.get(versions.size() - 1), url, skipCache);
            schemaInfoCache.put(tempUrl, SchemaInfoModel.builder()
                    .name(name)
                    .versions(versions)
                    .currSchema(currSchema)
                    .build());
        } else {
            log.debug("Hit info cache for: {}", tempUrl);
        }
        return schemaInfoCache.getIfPresent(tempUrl);
    }

    @Override
    public SchemaVersionModel getVersion(String name, int version, String url, boolean skipCache) throws SchemaRegistryRestException {
        url = String.format("%s/subjects/%s/versions/%d",
                Optional.ofNullable(kafkaEnvironmentsService.getCustomSchemaUrl(url)).orElse(this.schemaRegistryUrl),
                name,
                version
        );
        if (schemaVersionCache.getIfPresent(url) == null || skipCache) {
            NodeConverter<SchemaVersionModel> c = (node) -> {
                if (node.isObject()) {
                    return SchemaVersionModel.builder()
                            .id(node.path("id").asInt(-1))
                            .schema(node.path("schema").asText())
                            .subject(node.path("subject").asText())
                            .version(node.path("version").asInt(-1))
                            .build();
                }
                return null;
            };
            schemaVersionCache.put(url, proxyResponse(url, c, null));
        } else {
            log.debug("Hit version cache for: {}", url);
        }
        return schemaVersionCache.getIfPresent(url);
    }

    private <ResponseT> ResponseT proxyResponse(String url, NodeConverter<ResponseT> c, ResponseT defaultVal)
            throws SchemaRegistryRestException {
        try {
            ResponseEntity<String> res = restTemplate.getForEntity(url, String.class);
            if (!res.getStatusCode().is2xxSuccessful()) {
                log.error("Non 200 status: {}", res.getStatusCode());
                throw new SchemaRegistryRestException("Non 200 status: " + res.getStatusCode(), res.getStatusCodeValue());
            }
            ResponseT val = c.convert(MAPPER.readTree(res.getBody()));
            if (val == null) {
                return defaultVal;
            }
            return val;
        } catch (IOException e) {
            log.error("There was an error: {}", e.getMessage());
            throw new SchemaRegistryRestException(e.getMessage(), e, 500);
        }
    }

    public interface NodeConverter<ToT> extends Converter<JsonNode, ToT> { }

    public interface Converter<FromT, ToT> {
        ToT convert(FromT o);
    }
}
