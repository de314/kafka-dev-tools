package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/21/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProducerRepsonseModel {
    private int count;
    private long duration;
    private double rate;
    private boolean success;
    private boolean sent;
}
