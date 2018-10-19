package com.de314.kdt.config;

import com.de314.kdt.kakfa.ErrorTolerantAvroObjectDeserializer;
import com.de314.kdt.models.DeserializerInfoModel;
import com.de314.kdt.models.SerializerInfoModel;
import com.de314.kdt.services.DeserializerRegistryService;
import com.de314.kdt.services.SerializerRegistryService;
import com.de314.kdt.services.impl.KafkaDeserializerRegistryService;
import com.de314.kdt.services.impl.KafkaSerializerRegistryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;
import java.util.function.Function;

@Configuration
public class MappingConfig {

    public static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    @Bean
    public ObjectMapper jsonObjectMapper() {
        return JSON_OBJECT_MAPPER;
    }

    private static final String JSON_REPLACE;
    private static final String JSON_REPLACE_VAL = (char)191 + "";

    static {
        // http://stackoverflow.com/a/3020108
        String temp = "[";
        for (int i=0;i<=20;i++) {
            temp += (char)i;
        }
        JSON_REPLACE = temp + "]";
    }

    @Bean
    public SerializerRegistryService serializerRegistryService() {
        SerializerRegistryService registry = new KafkaSerializerRegistryService();

        registry.register(sim("string", StringSerializer.class, s -> s));
        registry.register(sim("bytes", ByteArraySerializer.class, s -> s));
        registry.register(sim("int", IntegerSerializer.class, s -> Integer.parseInt(s)));
        registry.register(sim("long", LongSerializer.class, s -> Long.parseLong(s)));

        return registry;
    }

    private SerializerInfoModel sim(String id, Class<?> serializerClass, Function<String, Object> rawPrep) {
        return sim(id, serializerClass.getSimpleName(), serializerClass, rawPrep);
    }

    private SerializerInfoModel sim(String id, String name, Class<?> serializerClass, Function<String, Object> rawPrep) {
        return SerializerInfoModel.builder()
                .id(UUID.randomUUID().toString())
                .name(name)
                .className(serializerClass.getName())
                .meta(Maps.newHashMap())
                .prepareRawFunc(rawPrep)
                .build();
    }

    @Bean
    public DeserializerRegistryService deserializerRegistryService() {
        DeserializerRegistryService registry = new KafkaDeserializerRegistryService();

        registry.register(dim("Avro Object Deserializer", ErrorTolerantAvroObjectDeserializer.class, "avro", (o) -> toNode(o + "")));
        registry.register(dim(StringDeserializer.class, "string", (o) -> new TextNode(o + "")));
        // TODO: JSON
//        registry.register(dim("Json Deserializer", GenericJsonDeserializer.class, "json", (o) -> {
//            try {
//                return toNode(JSON_OBJECT_MAPPER.writeValueAsString(o));
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//            return null;
//        }));
        registry.register(dim(ByteArrayDeserializer.class, "bytes", (o) -> toNode(w(Base64.getEncoder().encodeToString((byte[]) o)))));
        registry.register(dim(IntegerDeserializer.class, "int", (o) -> toNode(o + "")));
        registry.register(dim(LongDeserializer.class, "long", (o) -> toNode(o + "")));

        return registry;
    }

    private String w(String s) {
        return s == null ? "null" : "\"" + s + "\"";
    }

    private JsonNode toNode(String s) {
        try {
            return JSON_OBJECT_MAPPER.readTree(s.replace("\n", "\\n")
                    .replaceAll(JSON_REPLACE, JSON_REPLACE_VAL));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private DeserializerInfoModel dim(Class<?> deserializerClass, String id, Function<Object, JsonNode> prepOutput) {
        return dim(deserializerClass.getSimpleName(), deserializerClass, id, prepOutput);
    }

    private DeserializerInfoModel dim(String name, Class<?> deserializerClass, String id, Function<Object, JsonNode> prepOutput) {
        return DeserializerInfoModel.builder()
                .id(id)
                .name(name)
                .className(deserializerClass.getName())
                .prepareOutputFunc(prepOutput)
                .build();
    }
}
