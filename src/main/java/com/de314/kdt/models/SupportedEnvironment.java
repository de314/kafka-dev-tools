package com.de314.kdt.models;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 6/14/17.
 */
@Data
@NoArgsConstructor
public class SupportedEnvironment implements Model {

    private String id;
    private String name;
    private String kafkaHost;
    private String schemaUrl;
}
