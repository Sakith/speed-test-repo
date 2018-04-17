package com.sysco.speed.architecture.test.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.sysco.speed.architecture.service.ProducerResult;
import com.sysco.speed.architecture.service.impl.SyscoKinesisEventProducer;
import com.sysco.speed.architecture.service.utill.PropertyValuesReader;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertTrue;

/**
 * Created by charithap on 9/14/17.
 */
@PrepareForTest({SyscoKinesisEventProducer.class})
@RunWith(PowerMockRunner.class)
public class TestAmazonKinesisClient {

    private SyscoKinesisEventProducer syscoProducer;

    /**
     * Test for submitting an event to the Kinesis stream.
     */
    @Test
    public void testSubmitEvents(){

        try {
            AmazonKinesisClient producer = Mockito.mock(AmazonKinesisClient.class);

            long currentTime = System.currentTimeMillis();
            ByteBuffer data = ByteBuffer.wrap(String.format("testData-%d", currentTime).getBytes());

            PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");

            syscoProducer = new SyscoKinesisEventProducer(properties);
            Whitebox.invokeMethod(syscoProducer, "setProducer", producer);
            Whitebox.invokeMethod(syscoProducer, "setStreamName","testStreamName");

            PutRecordResult putRecordResult = new PutRecordResult();
            putRecordResult.setSequenceNumber("testStreamName");
            putRecordResult.setShardId("shardId");

            when(producer.putRecord(isA(PutRecordRequest.class)))
                    .thenReturn(putRecordResult);

            ProducerResult result = syscoProducer.submitEvent(data);
            verify(producer).putRecord(isA(PutRecordRequest.class));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSubmitEventsWithUnKnownHostException(){
        try{
            AmazonKinesisClient producer = Mockito.mock(AmazonKinesisClient.class);

            long currentTime = System.currentTimeMillis();
            ByteBuffer data = ByteBuffer.wrap(String.format("testData-%d", currentTime).getBytes());

            PropertyValuesReader propertyValuesReader = new PropertyValuesReader();
            Properties properties = propertyValuesReader.getPropertyValues("src/test/resources/TestApplicationConfig.properties");

            syscoProducer = new SyscoKinesisEventProducer(properties);
            Whitebox.invokeMethod(syscoProducer, "setProducer", producer);
            Whitebox.invokeMethod(syscoProducer, "setStreamName","testStreamName");

            PutRecordResult putRecordResult = new PutRecordResult();
            putRecordResult.setSequenceNumber("testStreamName");
            putRecordResult.setShardId("shardId");

            when(producer.putRecord(isA(PutRecordRequest.class))).thenThrow(new UnknownHostException());

            ProducerResult result = syscoProducer.submitEvent(data);
            verify(producer).putRecord(isA(PutRecordRequest.class));
            Assert.fail("Testing on fail scenario, should fail");
        }catch (Exception e){
            assertTrue(e instanceof MockitoException);
            assertTrue(e.getMessage().contains("UnknownHostException"));
        }
    }
}