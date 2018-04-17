package com.sysco.speed.architecture.service.impl.kinesis.configs;

import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.service.constants.EventConstants;
import com.sysco.speed.architecture.service.kinesis.configs.KinesisDefaultConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kinesis event producer configuration values stored in the defined property file.
 */
public class KinesisProducerConfigs extends KinesisDefaultConfigs {

    private static Logger LOGGER = LoggerFactory.getLogger(KinesisProducerConfigs.class);

    private static String DEFAULT_VALUE = "DEFAULT";
    private int maxNoOfConnections;
    private int connectionTimeout;
    private int socketTimeout;

    public KinesisProducerConfigs(Properties properties) throws ConfigurationException {
        super(properties);
        setMaxNoOfConnections(properties.getProperty(EventConstants.KinesisConstants.MAX_CONNECTIONS, DEFAULT_VALUE));
        setConnectionTimeout(properties.getProperty(EventConstants.KinesisConstants.CONNECTION_TIME_OUT, DEFAULT_VALUE));
        setSocketTimeout(properties.getProperty(EventConstants.KinesisConstants.SOCKET_TIME_OUT,DEFAULT_VALUE));
    }

    public int getMaxConnections() { return maxNoOfConnections; }

    public void setMaxNoOfConnections(String maxNoOfConnections) {
        if (DEFAULT_VALUE.equals(maxNoOfConnections)) {
            LOGGER.error("Maximum number of connections is not defined. Default value is used.");
        }
        this.maxNoOfConnections = Integer.parseInt(maxNoOfConnections);
    }

    public int getConnectionTimeout() { return connectionTimeout; }

    public void setConnectionTimeout(String connectionTimeout) {
        if (DEFAULT_VALUE.equals(connectionTimeout)) {
            LOGGER.error("Connection timeout is not defined. Default value is used.");
        }
        this.connectionTimeout = Integer.parseInt(connectionTimeout);
    }

    public int getSocketTimeout() { return socketTimeout; }

    public void setSocketTimeout(String socketTimeout) {
        if (DEFAULT_VALUE.equals(socketTimeout)) {
            LOGGER.error("Socket timeout is not defined. Default value is used.");
        }
        this.socketTimeout = Integer.parseInt(socketTimeout);
    }
}
