package com.sysco.speed.architecture.test.commons;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Created by charithap on 9/20/17.
 */
public class TestSchemaRegistry {
    //THE FOLLOWING TESTS HAVE BEEN COMMENTED DUE TO THE SCHEMAREGISTRY CLIENT INCOMPATIBILITY WITH 1.6
    @Test
    public void testSchemaRegistry(){
//        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
//        try {
//            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
//            SchemaRegistryDefaultConfigs configs = new SchemaRegistryDefaultConfigs(properties);
//            SyscoSchemaRegistryClient clientApp = new SyscoSchemaRegistryClient(configs);
//            String selectedSchemaVersionId = null;
//
//            Collection<SchemaVersionInfo> versions = clientApp.getAllSchemaVersions("TestSchema3");
//            for (SchemaVersionInfo s : versions) {
//               if (s.getId() == 1) {
//                    selectedSchemaVersionId = s.getId().toString();
//               }
//            }
//
//            if (selectedSchemaVersionId != null) {
//                String schema = clientApp.getSchemaBySchemaVersionId("1");
//                String sampleSchema = "{\"type\":\"record\",\"namespace\":\"SampleTest\",\"name\":\"Order\",\"fields\":[{\"name\":\"OrderId\",\"type\":\"string\"},{ \"name\":\"OrderType\",\"type\":\"string\"}]}";
//                assertEquals(sampleSchema, schema);
//            } else {
//                System.out.print("No schema versions found to use");
//            }
//
//
//
//        } catch (IOException e){
//            e.printStackTrace();
//        }
    }

    @Test
    public void testRegisterSchema() {
//        try {
//            PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
//            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
//            SyscoSchemaRegistryClient clientApp = new SyscoSchemaRegistryClient(new SchemaRegistryDefaultConfigs(properties));
//
//            //String json = "{ \"OrderId\": \"111111\", \"OrderType\": \"Pick-up\"}";
//            String sampleSchema = "{\"type\":\"record\",\"namespace\":\"SampleTest\",\"name\":\"Order\",\"fields\":[{\"name\":\"OrderId\",\"type\":\"string\"},{ \"name\":\"OrderType\",\"type\":\"string\"}]}";
//
//
//            SchemaMetadata.Builder metadata = new SchemaMetadata.Builder("TestSchema8");
//            metadata.description("Test Description");
//            metadata.evolve(true);
//            metadata.type("avro");
//            metadata.validationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL);
//            metadata.compatibility(SchemaCompatibility.BACKWARD);
//            metadata.schemaGroup("TestSchemaGroup");
//
//            Long schemaId = clientApp.registerSchema(metadata.build(), sampleSchema);
//
//            System.out.print(schemaId);
//        }
//        catch (IOException e){
//            e.printStackTrace();
//        }

    }
}
