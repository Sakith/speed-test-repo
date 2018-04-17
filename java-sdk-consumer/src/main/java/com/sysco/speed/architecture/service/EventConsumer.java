package com.sysco.speed.architecture.service;

import java.net.UnknownHostException;

/**
 * Event consumer implementations should implement this interface
 *
 * Created by nuwanp on 7/21/17.
 */
public interface EventConsumer {

    /**
     * Continuous processing of events is triggered.
     *
     * @throws UnknownHostException
     */
    void processEvents(String schemaId) throws UnknownHostException;

    /**
     * Continuous processing of events is triggered.
     *
     * @throws UnknownHostException
     */
    void processEvents() throws UnknownHostException;
}
