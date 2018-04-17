package com.sysco.speed.architecture.test.kinesis;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.sysco.speed.architecture.service.ConsumerResult;
import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.EventConsumer;
import com.sysco.speed.architecture.service.EventConsumerFactory;
import com.sysco.speed.architecture.service.EventConsumerType;
import com.sysco.speed.architecture.service.impl.KinesisEventConsumer;
import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisConsumerConfigs;
import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import com.sysco.speed.architecture.service.utill.Observer;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Test cases to check event consumer implementation
 *
 * Created by nuwanp on 7/24/17.
 */
public class TestKinesisEventConsumer {

    private static Log LOGGER = LogFactory.getLog(TestKinesisEventConsumer.class);

    /**
     * Test case to check the Event Consumer Factory functionality
     *
     */
    @Test
    public void testEventConsumerFactory() {

        class testConsumerListenerImplementation implements Observer {

            private testConsumerListenerImplementation(){}

            public void onDataReceived(ConsumerResult data){
                LOGGER.info("Data reached consumer level!!! : " + data.getData());
            }
        }

        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();

        Properties properties = null;
        try{
            properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
        }catch (IOException e){
            Assert.fail("IO exception have occurred in property loading testing", e);
        }

        try {
            EventConsumer eventConsumer = EventConsumerFactory
                    .getEventConsumer(EventConsumerType.KINESIS, properties, new testConsumerListenerImplementation());
            assertEquals(true, eventConsumer instanceof KinesisEventConsumer,
                    "Kinesis event consumer expected instance not found");
        }catch(ConfigurationException e){
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test case to check the Kinesis event consumer configuration object
     *
     * @throws IOException
     */
    @Test
    public void testKinesisEventConsumerConfig() {
        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();

        Properties properties = null;
        try{
            properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");
        }catch (IOException e){
            Assert.fail("IO exception have occurred in property loading testing", e);
        }

        try {
            KinesisConsumerConfigs kinesisEventConsumerConfig = new KinesisConsumerConfigs(properties);
            assertEquals(kinesisEventConsumerConfig.getApplicationName(), "UOM-client-app1");
            assertEquals(kinesisEventConsumerConfig.getRetriesNum(), 10);
            assertEquals(kinesisEventConsumerConfig.getBackoffTimeInMillis(), 3000L);
        }catch(ConfigurationException e){
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testEventConsumption(){

        LOGGER.info("Execution of testEventConsumption()");

        class testConsumerListenerImplementation implements Observer {

            private testConsumerListenerImplementation(){}

            public void onDataReceived(ConsumerResult data){
                LOGGER.info("Data reached consumer level!!! : " + data.getData());
            }
        }

        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        Properties properties = null;
        String filePath = "src/test/resources/TestApplicationConfig.properties";
        KinesisConsumerConfigs kinesisEventConsumerConfig = null;

        try{
            properties = propertyValuesReader.getPropertyValues(filePath);
        }catch (IOException e){
            Assert.fail("IO exception have occurred", e);
        }

        try {
            kinesisEventConsumerConfig = new KinesisConsumerConfigs(properties);
        }catch(ConfigurationException e){
            Assert.fail(e.getMessage());
        }

        SyscoSchemaRegistryClient schemaClient = new SyscoSchemaRegistryClient(new SchemaRegistryDefaultConfigs(properties));

        EventConsumer consumer = new KinesisEventConsumer(kinesisEventConsumerConfig, schemaClient, new testConsumerListenerImplementation());
        try {
            consumer.processEvents();
        } catch (UnknownHostException e) {
            LOGGER.error(e);
            Assert.fail("Unknown host exception has thrown during the test");
        }

    }

    /**
     *
     */
    @Test
    public void testEventConsumptionWithSchema(){

         class testConsumerListenerImplementation implements Observer {

             private testConsumerListenerImplementation(){}

             public void onDataReceived(ConsumerResult data){
                 LOGGER.info("Data reached consumer level!!! : " + data.getData());
             }
        }

        LOGGER.info("Execution of testEventConsumption() with schema");

        PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
        Properties properties = null;
        String filePath = "src/test/resources/TestApplicationConfig.properties";
        KinesisConsumerConfigs kinesisEventConsumerConfig = null;

        try{
            properties = propertyValuesReader.getPropertyValues(filePath);
        }catch (IOException e){
            Assert.fail("IO exception have occurred", e);
        }

        try {
            kinesisEventConsumerConfig = new KinesisConsumerConfigs(properties);
        }catch(ConfigurationException e){
            Assert.fail(e.getMessage());
        }
        SyscoSchemaRegistryClient schemaClient = new SyscoSchemaRegistryClient(new SchemaRegistryDefaultConfigs(properties));

        EventConsumer consumer = new KinesisEventConsumer(kinesisEventConsumerConfig, schemaClient, new testConsumerListenerImplementation());
        try {
            String schemaVersionId = schemaClient.getSchemaVersionId("TestSchema1", 1);

            consumer.processEvents(schemaVersionId);
        } catch (UnknownHostException e) {
            LOGGER.error(e);
            Assert.fail("Unknown host exception has thrown during the test");
        }

    }
}
