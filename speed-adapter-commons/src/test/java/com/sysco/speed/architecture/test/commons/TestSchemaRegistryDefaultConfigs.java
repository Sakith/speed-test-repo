package com.sysco.speed.architecture.test.commons;

import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Created by charithap on 9/21/17.
 */
public class TestSchemaRegistryDefaultConfigs {
    @Test
    public void testSchemaRegistryDefaultConfigs() {
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        try {
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
            SchemaRegistryDefaultConfigs testConfigs = new SchemaRegistryDefaultConfigs(properties);

            assertEquals(testConfigs.getSchemaRegistryApiUrl(),"ADD_SCHEMA_REGISTRY_URL");

            testConfigs.setSchemaRegistryApiUrl("TestSchemaRegirtyUrl");

            assertEquals(testConfigs.getSchemaRegistryApiUrl(),"TestSchemaRegirtyUrl");

        } catch (IOException e){
            Assert.fail("Failed to read the schema registry configurations", e);
        }
    }
}
