package com.sysco.speed.architecture.service;

import java.nio.ByteBuffer;

/**
 * Created by chamantha on 7/19/17.
 */
public interface SyscoEventProducer {

    ProducerResult submitJsonEvent(String json, String schemaId);

    ProducerResult submitEvent(ByteBuffer data);
}
