package com.de314.kdt.services;

/**
 * Created by davidesposito on 7/6/16.
 */
public interface JsonToAvroConverter {

    Object convert(String json, String schemaStr);
}
