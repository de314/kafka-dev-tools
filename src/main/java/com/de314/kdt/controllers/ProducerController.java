package com.de314.kdt.controllers;

import com.de314.kdt.config.KafkaDevToolsConfig;
import com.de314.kdt.models.KafkaProduceMessageMetaModel;
import com.de314.kdt.models.KafkaProduceRequestModel;
import com.de314.kdt.models.ProducerRepsonseModel;
import com.de314.kdt.models.SerializerInfoModel;
import com.de314.kdt.services.JsonToAvroConverter;
import com.de314.kdt.services.KafkaProducerService;
import com.de314.kdt.services.SerializerRegistryService;
import com.de314.kdt.utils.Opt;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.AvroTypeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
@RequestMapping("/api/v1/producers")
public class ProducerController {

    private final SerializerRegistryService serializerRegistryService;
    private final KafkaProducerService kafkaProducerService;
    private final JsonToAvroConverter jtaConverter;
    private final ObjectMapper jsonObjectMapper;
    private final boolean producersEnabeled;

    public ProducerController(SerializerRegistryService serializerRegistryService,
            KafkaProducerService kafkaProducerService,
            JsonToAvroConverter jtaConverter,
            ObjectMapper jsonObjectMapper,
            KafkaDevToolsConfig kafkaDevToolsConfig) {
        this.serializerRegistryService = serializerRegistryService;
        this.kafkaProducerService = kafkaProducerService;
        this.jtaConverter = jtaConverter;
        this.jsonObjectMapper = jsonObjectMapper;
        this.producersEnabeled = kafkaDevToolsConfig.getProducer().isEnabled();
    }

    @PostMapping("/publish")
    public ResponseEntity<ProducerRepsonseModel> publish(@RequestBody KafkaProduceRequestModel req,
            @RequestParam("count") Optional<Integer> oCount) {
        if (!producersEnabeled) {
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
                    .header("KDT-Error", "The producer feature is not enabled.")
                    .body(null);
        }
        String serId = req.getMeta().getSerializerId();
        if (serId == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Missing serializer id")
                    .body(null);
        }
        SerializerInfoModel ser = serializerRegistryService.findById(serId);
        if (ser == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Invalid serializer id")
                    .body(null);
        }
        return ResponseEntity.ok(
                kafkaProducerService.produce(req, ser, oCount.orElse(1))
        );
    }
}
