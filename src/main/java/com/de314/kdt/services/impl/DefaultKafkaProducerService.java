package com.de314.kdt.services.impl;

import com.de314.kdt.models.KafkaProduceMessageMetaModel;
import com.de314.kdt.models.KafkaProduceRequestModel;
import com.de314.kdt.models.ProducerRepsonseModel;
import com.de314.kdt.models.SerializerInfoModel;
import com.de314.kdt.models.SupportedEnvironment;
import com.de314.kdt.services.KafkaEnvironmentRegistryService;
import com.de314.kdt.services.KafkaProducerService;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.tomcat.util.http.ResponseUtil;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

@Slf4j
@Service
public class DefaultKafkaProducerService implements KafkaProducerService {

    private final KafkaEnvironmentRegistryService environmentRegistryService;

    private final Map<String, EnvironmentProducer> repo;

    public DefaultKafkaProducerService(KafkaEnvironmentRegistryService environmentRegistryService) {
        this.environmentRegistryService = environmentRegistryService;
        this.repo = Maps.newConcurrentMap();
    }

    @Override
    public ProducerRepsonseModel produce(KafkaProduceRequestModel req, SerializerInfoModel serializer, int count) {
        KafkaProduceMessageMetaModel meta = req.getMeta();
        String id = meta.getId();

        if (!repo.containsKey(id)) {
            SupportedEnvironment env = environmentRegistryService.findById(meta.getEnvironmentId());
            repo.put(id, new EnvironmentProducer(env, serializer.getClassName()));
        }
        EnvironmentProducer envProducer = repo.get(req.getMeta().getId());

        Object message = serializer.getPrepareRawFunc().apply(req.getRawMessage(), req);

        boolean sendMessage = req.getRawMessage() != null && envProducer != null;

        ProducerRepsonseModel res = ProducerRepsonseModel.builder()
                .sent(sendMessage)
                .count(0)
                .success(false)
                .duration(-1)
                .rate(-1)
                .build();
        if (sendMessage) {
            res = sendMessage(envProducer, req.getTopic(), req.getKey(), message, count);
        }
        return res;
    }

    private ProducerRepsonseModel sendMessage(EnvironmentProducer envProducer, String topic, String key, Object payload, int count) {
        int success = 0;
        long duration;
        double rate;
        long startTime = System.currentTimeMillis();
        for (int i=0;i<count;i++) {
            try {
                envProducer.send(topic, key, payload);
                success++;
            } catch (Exception e) {
                e.printStackTrace();
                log.error("There was an error: {}", e);
            }
        }
        log.trace("/publish Done sending ({}/{}) on {}", success, count, topic);
        duration = System.currentTimeMillis() - startTime;
        rate = count * 1000.0 / duration;
        return ProducerRepsonseModel.builder()
                .count(success)
                .duration(duration)
                .rate(rate)
                .sent(true)
                .success(success > 0 && success == count)
                .build();
    }

    @Data
    private static class EnvironmentProducer {

        public static final Pattern NUMBER_KEY_PATTERN = Pattern.compile("^\\d+$");

        private final AtomicLong sentCount;
        private final AtomicLong errorCount;
        private final KafkaProducer<String, Object> producer;

        public EnvironmentProducer(SupportedEnvironment environment, String valueSerializer) {
            this.sentCount = new AtomicLong(0L);
            this.errorCount = new AtomicLong(0L);

            final Properties properties = new Properties();
            properties.put("acks", "0");
            properties.put("bootstrap.servers", environment.getKafkaHost());
            properties.put("schema.registry.url", environment.getSchemaUrl());
            properties.put("key.serializer", StringSerializer.class);
            properties.put("value.serializer", valueSerializer);

            this.producer = new KafkaProducer<>(properties);
        }

        public long getSentCount() {
            return sentCount.get();
        }

        public long getErrorCount() {
            return errorCount.get();
        }

        public void send(String topic, String key, Object val) {
            if (this.producer != null) {
                Integer partition = null;
                if (NUMBER_KEY_PATTERN.matcher(key).find()) {
                    partition = Integer.parseInt(key);
                }
                ProducerRecord<String, Object> pr = new ProducerRecord<>(topic, partition, key, val);
                try {
                    producer.send(pr);
                    sentCount.getAndIncrement();
                } catch (Exception e) {
                    errorCount.getAndIncrement();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
