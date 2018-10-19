package com.de314.kdt.kakfa;

import com.de314.kdt.utils.Opt;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Created by davidesposito on 7/20/16.
 */
@Slf4j
public class ErrorTolerantAvroObjectDeserializer extends KafkaAvroDeserializer {

    protected Object deserialize(boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload, Schema readerSchema) throws SerializationException {
        String error;
        try {
            return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, readerSchema);
        } catch (SerializationException e) {
            if (TopicErrorMap.shouldKill(TopicErrorMap.count(topic, "avro"))) {
                // TODO: environment
                log.error("Topic {} reached it's avro error threshold. Recommended for killing.");
            } else {
                log.warn("There was an error deserializing message on {}, caused by: {}", topic, e.getCause());
            }
            error = String.format("\" \nThere was an error deserializing the avro payload on \"{}\". \n \nError: %s \n\"",
                    topic, Opt.of(e.getCause()).map(cause -> "\n \nCause: " + cause.getMessage()).orElse(""));
        }
        return error;
    }
}
