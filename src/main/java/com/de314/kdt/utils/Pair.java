package com.de314.kdt.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Pair<L, R> {

    private L left;
    private R right;

    public static <SL, SR> Pair<SL, SR> of(SL first, SR second) {
        return new Pair<>(first, second);
    }
}
