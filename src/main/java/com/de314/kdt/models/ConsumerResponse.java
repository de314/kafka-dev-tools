package com.de314.kdt.models;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConsumerResponse {
    private final String consumerId;
    private final Page<JsonNode> page;
}
