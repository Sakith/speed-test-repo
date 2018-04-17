package com.sysco.speed.architecture.service;

import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.impl.KinesisEventConsumer;
import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisConsumerConfigs;
import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import com.sysco.speed.architecture.service.utill.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Factory class to deliver required event consumers
 *
 * Created by nuwanp on 7/21/17.
 */
public class EventConsumerFactory {

    private static Logger LOGGER = LoggerFactory.getLogger(EventConsumerFactory.class);

    /**
     * {@link EventConsumer} instance for the specified {@link EventConsumerType} will be returned.
     *
     * @param eventType
     * @return
     */

    public static EventConsumer getEventConsumer(EventConsumerType eventType, Properties properties, Observer observer) throws ConfigurationException {
        switch (eventType) {
        case KINESIS:
            return new KinesisEventConsumer(new KinesisConsumerConfigs(properties), new SyscoSchemaRegistryClient(new SchemaRegistryDefaultConfigs(properties)), observer);
        }
        throw new UnsupportedOperationException("Event consumer type " + eventType + " is not supported");
    }

}
