package com.sysco.speed.architecture.service.impl.kinesis.configs;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.service.constants.EventConstants;
import com.sysco.speed.architecture.service.kinesis.configs.KinesisDefaultConfigs;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Kinesis event consumer configuration values stored from the defined property file.
 * <p>
 * Created by nuwanp on 7/24/17.
 */
public class KinesisConsumerConfigs extends KinesisDefaultConfigs {

    private static Logger LOGGER = LoggerFactory.getLogger(KinesisConsumerConfigs.class);

    private static String DEFAULT_VALUE = "DEFAULT";
    private String applicationName;
    private long backoffTimeInMillis;
    private int retriesNum;
    private long checkpointIntervalMillis;
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;
    private Date timestamp;

    public KinesisConsumerConfigs(Properties properties) throws ConfigurationException {
        super(properties);
        setApplicationName(properties.getProperty(EventConstants.KinesisConstants.APPLICATION_NAME, DEFAULT_VALUE));
        setInitialPositionInStream(
                properties.getProperty(EventConstants.KinesisConstants.INITIAL_STREAM_POSITION, DEFAULT_VALUE));
        setBackoffTimeInMillis(properties.getProperty(EventConstants.KinesisConstants.BACKOFF_TIME_IN_MILLIS, "3000"));
        setCheckpointIntervalMillis(properties.getProperty(EventConstants.KinesisConstants.CHECKPOINT_INTERVAL_MILLIS, "6000"));
        setRetriesNum(properties.getProperty(EventConstants.KinesisConstants.NUM_RETRIES, "10"));

        if (getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)){
            setTimestamp(properties.getProperty(EventConstants.KinesisConstants.TIMESTAMP, null));
        }
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) throws ConfigurationException {

        if (DEFAULT_VALUE.equals(applicationName)) {
            throw new ConfigurationException("[ERROR] Missing configuration for application name");
        }
        this.applicationName = applicationName;
    }

    public InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    public void setInitialPositionInStream(String initialPositionInStream) {
        if ("AT_TIMESTAMP".equalsIgnoreCase(initialPositionInStream)) {
            this.initialPositionInStream = InitialPositionInStream.AT_TIMESTAMP;
        } else if ("TRIM_HORIZON".equalsIgnoreCase(initialPositionInStream)) {
            this.initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;
        } else {
            // Default value is latest
            LOGGER.info("Initial position in stream not defined. LATEST is used as the default value");
            this.initialPositionInStream = InitialPositionInStream.LATEST;
        }
    }

    public long getBackoffTimeInMillis() {
        return backoffTimeInMillis;
    }

    public void setBackoffTimeInMillis(String backoffTimeInMillis) throws ConfigurationException {
        try {
            this.backoffTimeInMillis = Long.parseLong(backoffTimeInMillis);
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Backoff time property is expecting a long value.");
        }
    }

    public int getRetriesNum() {
        return retriesNum;
    }

    public void setRetriesNum(String retriesNum) throws ConfigurationException {
        try {
            this.retriesNum = Integer.parseInt(retriesNum);
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Retries number property is expecting an integer value.");
        }
    }

    public long getCheckpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    public void setCheckpointIntervalMillis(String checkpointIntervalMillis) throws ConfigurationException {
        try {
            this.checkpointIntervalMillis = Long.parseLong(checkpointIntervalMillis);
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Checkpoint interval property is expecting a long value.");
        }

    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) throws ConfigurationException {
        if (StringUtils.isNotEmpty(timestamp)) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
                this.timestamp = formatter.parse(timestamp.replaceAll("Z$", "+0000"));
            } catch (ParseException e) {
                throw new ConfigurationException("Timestamp property can't be passed.");
            }
        } else {
            throw new ConfigurationException("Timestamp property must be set in the configuration file !");
        }
    }
}
