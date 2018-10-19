package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Created by davidesposito on 7/18/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaInfoModel {
    private String name;
    private List<Integer> versions;
    private SchemaVersionModel currSchema;
}
