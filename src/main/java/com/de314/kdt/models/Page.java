package com.de314.kdt.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Created by davidesposito on 7/20/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Page<T> {

    private List<T> content;
    private int page;
    private int size;
    private long totalElements;
}
