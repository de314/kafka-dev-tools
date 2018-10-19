package com.de314.kdt.kakfa;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class TopicErrorMap {

    private static final Map<String, Map<String, AtomicLong>> ERROR_MAP = Maps.newConcurrentMap();

    public static long count(String topic, String format) {
        if (!ERROR_MAP.containsKey(topic)) {
            ERROR_MAP.put(topic, Maps.newConcurrentMap());
        }
        Map<String, AtomicLong> topicMap = ERROR_MAP.get(topic);
        if (!topicMap.containsKey(format)) {
            topicMap.put(format, new AtomicLong(0));
        }
        return topicMap.get(format).incrementAndGet();
    }

    public static long peek(String topic, String format) {
        return Optional.ofNullable(ERROR_MAP.get(topic))
                .map(topicMap -> topicMap.get(format))
                .map(count -> count.get())
                .orElse(0L);
    }

    public static boolean shouldKill(String topic, String format) {
        return shouldKill(peek(topic, format));
    }

    public static boolean shouldKill(long count) {
        return count > 1000;
    }

    public static void clear(String topic, String format) {
        Optional.ofNullable(ERROR_MAP.get(topic))
                .map(topicMap -> topicMap.get(format))
                .ifPresent(count -> count.set(0));
    }
}
