package com.sysco.speed.architecture.test.commons;
import static org.mockito.Mockito.*;

import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import com.sysco.speed.architecture.service.kinesis.configs.KinesisDefaultConfigs;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by charithap on 9/14/17.
 */
public class TestKinesisDefaultConfigs {
    /**
     * Test KinesisDefaulConfigsObject
     *
     */
    @Test
    public void testKinesisDefaultConfigs() {
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        try {
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
            KinesisDefaultConfigs testConfigs = new KinesisDefaultConfigs(properties);

            assertEquals(testConfigs.getAccessKey(),"ADD_KEY_TO_TEST");
            assertEquals(testConfigs.getSecretKey(),"ADD_KEY_TO_TEST");
            assertEquals(testConfigs.getStreamName(),"ADD_STREAM_NAME");

            testConfigs.setAccessKey("TestAccessKey");
            testConfigs.setSecretKey("TestSecretKey");
            testConfigs.setStreamName("TestStreamName");

            assertEquals(testConfigs.getAccessKey(),"TestAccessKey");
            assertEquals(testConfigs.getSecretKey(),"TestSecretKey");
            assertEquals(testConfigs.getStreamName(),"TestStreamName");

        } catch (IOException e){
            Assert.fail("Failed to read the default property values", e);
        } catch (ConfigurationException e) {
            Assert.fail("Failed to set the Kinesis configurations", e);
        }
    }
}
