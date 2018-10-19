package com.de314.kdt.services;

import com.de314.kdt.models.KafkaProduceRequestModel;
import com.de314.kdt.models.ProducerRepsonseModel;
import com.de314.kdt.models.SerializerInfoModel;

public interface KafkaProducerService {

    ProducerRepsonseModel produce(KafkaProduceRequestModel req, SerializerInfoModel serializer, int count);
}
