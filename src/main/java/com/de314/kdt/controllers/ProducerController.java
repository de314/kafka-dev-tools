package com.de314.kdt.controllers;

import com.de314.kdt.config.KafkaDevToolsConfig;
import com.de314.kdt.models.KafkaProduceRequestModel;
import com.de314.kdt.models.ProducerRepsonseModel;
import com.de314.kdt.models.SerializerInfoModel;
import com.de314.kdt.services.KafkaProducerService;
import com.de314.kdt.services.SerializerRegistryService;
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
    private final boolean producersEnabeled;

    public ProducerController(SerializerRegistryService serializerRegistryService,
            KafkaProducerService kafkaProducerService, KafkaDevToolsConfig kafkaDevToolsConfig) {
        this.serializerRegistryService = serializerRegistryService;
        this.kafkaProducerService = kafkaProducerService;
        this.producersEnabeled = kafkaDevToolsConfig.getProducer().isEnabled();
    }

    @PostMapping("/publish")
    public ResponseEntity<ProducerRepsonseModel> publish(@RequestBody KafkaProduceRequestModel requestModel,
            @RequestParam("count") Optional<Integer> oCount) {
        if (!producersEnabeled) {
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
                    .header("KDT-Error", "The producer feature is not enabled.")
                    .body(null);
        }
        if (requestModel.getMeta().getSerializerId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Missing serializer id")
                    .body(null);
        }
        SerializerInfoModel ser = serializerRegistryService.findById(requestModel.getMeta().getSerializerId());
        if (ser == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("KDT-Error", "Invalid serializer id")
                    .body(null);
        }

        return ResponseEntity.ok(
                kafkaProducerService.produce(requestModel, ser, oCount.orElse(1))
        );
    }
}
