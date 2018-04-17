package com.sysco.speed.architecture.test.kinesis;

import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.ProducerResult;
import com.sysco.speed.architecture.service.impl.SyscoKinesisEventProducer;

import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Properties;

/**
 * Created by chamantha on 7/24/17.
 */
public class TestKinesisEventHandling {

    private static Logger LOGGER = LoggerFactory.getLogger(SyscoKinesisEventProducer.class);

    SyscoKinesisEventProducer producer;
    SyscoSchemaRegistryClient schemaClient;
    Properties properties;

    @BeforeTest
    public void initializeSyscoKinesisEventProducer() throws IOException, ConfigurationException {

        // Read Kinesis producer parameter values from property file.
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        this.properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");

        // Create client for accessing Kinesis.
        producer = new SyscoKinesisEventProducer(properties);
    }

    /**
     * Test for submitting an event to the Kinesis stream.
     */
    @Test
    public void testSubmitEvent(){

        // Producer submitting a mock data record.
        long currentTime = System.currentTimeMillis();
        try{
            ProducerResult result = producer.submitEvent(ByteBuffer.wrap(String.format("testData-BSE-testing").getBytes()));
            LOGGER.info("Event is submitted successfully.");
        }catch (RuntimeException e){
            Assert.fail("Testing on success scenario, should not fail", e);
        }
    }

    /**
     * Test for submitting an event with size greater than 1 MB.
     */
    @Test
    public void testSubmitEventWithLargePayload(){

        // Producer submitting a mock data record which is larger than 1MB.
        StringBuilder sb = new StringBuilder(2000000);
        for (int i=0; i<2000000; i++) sb.append('n');
        try{
            ProducerResult result = producer.submitEvent(ByteBuffer.wrap(sb.toString().getBytes()));
            Assert.fail("Testing on fail scenario, should fail");
        }catch (RuntimeException e){
        }
    }

    /**
     * Test for submitting an event (in JSON format) to the Kinesis stream.
     */
    @Test
    public void testSubmitJsonEvent(){

        this.schemaClient = new SyscoSchemaRegistryClient(new SchemaRegistryDefaultConfigs(properties));

        String schemaVersionId = schemaClient.getSchemaVersionId("TestSchema", 1);

        // Producer submitting a mock data record.
        String json = "{ \"OrderId\": \"111111\", \"OrderType\": \"Pick-up\"}";

        try{
            producer.submitJsonEvent(json, schemaVersionId);
            LOGGER.info("Event is submitted successfully.");
        }catch (Exception e){
            Assert.fail("Testing on success scenario, should not fail", e);
        }
    }
}
