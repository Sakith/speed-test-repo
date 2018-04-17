package com.sysco.speed.architecture.test.kinesis;

import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisProducerConfigs;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Created by charithap on 9/14/17.
 */
public class TestKinesisProducerConfigs {
    @Test
    public void testKinesisProducerConfigs() {
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        try {
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
            KinesisProducerConfigs testConfigs = new KinesisProducerConfigs(properties);

            assertEquals(testConfigs.getAccessKey(),"ADD_ACCESS_KEY_HERE");
            assertEquals(testConfigs.getSecretKey(),"ADD_SECRET_KEY_HERE");
            assertEquals(testConfigs.getStreamName(),"ADD_STREAM_NAME_HERE");
            assertEquals(testConfigs.getConnectionTimeout(),60000);
            assertEquals(testConfigs.getMaxConnections(),25);
            assertEquals(testConfigs.getSocketTimeout(),60000);

            testConfigs.setAccessKey("TestAccessKey");
            testConfigs.setSecretKey("TestSecretKey");
            testConfigs.setStreamName("TestStreamName");
            testConfigs.setConnectionTimeout("65000");
            testConfigs.setMaxNoOfConnections("30");
            testConfigs.setSocketTimeout("65000");

            assertEquals(testConfigs.getAccessKey(),"TestAccessKey");
            assertEquals(testConfigs.getSecretKey(),"TestSecretKey");
            assertEquals(testConfigs.getStreamName(),"TestStreamName");
            assertEquals(testConfigs.getConnectionTimeout(),65000);
            assertEquals(testConfigs.getMaxConnections(),30);
            assertEquals(testConfigs.getSocketTimeout(),65000);

        } catch (IOException e){
            Assert.fail("Failed to read the producer's property values", e);
        } catch (ConfigurationException e) {
            Assert.fail("Failed to set the Kinesis Producer configurations", e);
        }
    }
}
