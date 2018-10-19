package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/18/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaVersionModel {
    private int id;
    private String schema;
    private String subject;
    private int version;
}
