package com.sysco.speed.architecture.test.kinesis;


import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisConsumerConfigs;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Created by charithap on 9/14/17.
 */
public class TestKinesisConsumerConfigs {
    @Test
    public void testKinesisConsumerConfigs() {
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        try {
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
            KinesisConsumerConfigs testConfigs = new KinesisConsumerConfigs(properties);

            assertEquals(testConfigs.getAccessKey(),"ADD_ACCESS_KEY");
            assertEquals(testConfigs.getSecretKey(),"ADD_SECRET_KEY");
            assertEquals(testConfigs.getStreamName(),"ADD_STREAM_NAME");
            assertEquals(testConfigs.getApplicationName(),"ADD_APPLICATION_NAME");
            assertEquals(testConfigs.getBackoffTimeInMillis(),3000);
            assertEquals(testConfigs.getCheckpointIntervalMillis(),6000);
            assertEquals(testConfigs.getInitialPositionInStream(), InitialPositionInStream.LATEST);
            assertEquals(testConfigs.getRetriesNum(),10);

            if (testConfigs.getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)) {
                assertEquals(testConfigs.getTimestamp(), "ADD_TIMESTAMP");
            }

            testConfigs.setAccessKey("TestAccessKey");
            testConfigs.setSecretKey("TestSecretKey");
            testConfigs.setStreamName("TestStreamName");
            testConfigs.setApplicationName("TestApplicationName");
            testConfigs.setApplicationName("TestApplicationName");
            testConfigs.setBackoffTimeInMillis("4000");
            testConfigs.setCheckpointIntervalMillis("5000");
            testConfigs.setInitialPositionInStream("AT_TIMESTAMP");
            testConfigs.setRetriesNum("20");
            testConfigs.setTimestamp("TimeInGMTFormat");

            assertEquals(testConfigs.getAccessKey(),"TestAccessKey");
            assertEquals(testConfigs.getSecretKey(),"TestSecretKey");
            assertEquals(testConfigs.getStreamName(),"TestStreamName");
            assertEquals(testConfigs.getApplicationName(),"TestApplicationName");
            assertEquals(testConfigs.getBackoffTimeInMillis(),4000);
            assertEquals(testConfigs.getCheckpointIntervalMillis(),5000);
            assertEquals(testConfigs.getInitialPositionInStream(), InitialPositionInStream.AT_TIMESTAMP);
            assertEquals(testConfigs.getRetriesNum(),20);
            assertEquals(testConfigs.getTimestamp().toString(),"TimeInGMTFormat");

            testConfigs.setInitialPositionInStream("TRIM_HORIZON");
            assertEquals(testConfigs.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);

            testConfigs.setInitialPositionInStream("IncorrectPosition");
            assertEquals(testConfigs.getInitialPositionInStream(), InitialPositionInStream.LATEST);

        } catch (IOException e){
            Assert.fail("Failed to read the consumer's property values", e);
        } catch (ConfigurationException e) {
            Assert.fail("Failed to set the Kinesis Consumer configurations", e);
        }
    }
}
