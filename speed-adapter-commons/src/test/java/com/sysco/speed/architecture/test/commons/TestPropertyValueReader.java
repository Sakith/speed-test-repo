package com.sysco.speed.architecture.test.commons;

import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Test cases to check Property value read implementation
 *
 * Created by nuwanp on 7/25/17.
 */
public class TestPropertyValueReader {

    /**
     * Test case to check the property file read utility function
     *
     */
    @Test public void testPropertyValuesReader() {
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        try {
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
            assertEquals(properties.getProperty("application.name"),"ADD_APPLICATION_NAME");
            assertEquals(properties.getProperty("stream.name"),"ADD_STREAM_NAME");
            assertEquals(properties.getProperty("access.key"),"ADD_KEY_TO_TEST");
            assertEquals(properties.getProperty("schema.registry.api.url"),"ADD_SCHEMA_REGISTRY_URL");
        } catch (IOException e) {
            assertEquals(false,true,e.getMessage());
        }
    }
}
