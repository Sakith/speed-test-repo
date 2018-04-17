package com.sysco.speed.architecture.service.kinesis.configs;

import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.service.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kinesis event consumer configuration values stored from the defined property file.
 * <p>
 * Created by Chamantha 7/27/2017
 */
public class KinesisDefaultConfigs {

    private static Logger LOGGER = LoggerFactory.getLogger(KinesisDefaultConfigs.class);

    private static String DEFAULT_VALUE = "DEFAULT";
    private String streamName;

    private String accessKey;
    private String secretKey;

    public KinesisDefaultConfigs(Properties properties) throws ConfigurationException {
        setStreamName(properties.getProperty(EventConstants.KinesisConstants.STREAM_NAME, DEFAULT_VALUE));
        setAccessKey(properties.getProperty(EventConstants.KinesisConstants.ACCESS_KEY, DEFAULT_VALUE));
        setSecretKey(properties.getProperty(EventConstants.KinesisConstants.SECRET_KEY, DEFAULT_VALUE));
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) throws ConfigurationException {

        if (DEFAULT_VALUE.equals(streamName)) {
            throw new ConfigurationException("[ERROR] Missing configuration for stream name");
        }
        this.streamName = streamName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) throws ConfigurationException {

        if (DEFAULT_VALUE.equals(accessKey)) {
            throw new ConfigurationException("[ERROR] Missing configuration for access key");
        }
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) throws ConfigurationException {

        if (DEFAULT_VALUE.equals(secretKey)) {
            throw new ConfigurationException("[ERROR] Missing configuration for secret key");
        }
        this.secretKey = secretKey;
    }

}
