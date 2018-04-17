package com.sysco.speed.architecture.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.sysco.speed.architecture.exceptions.ConfigurationException;
import com.sysco.speed.architecture.exceptions.SchemaException;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.SyscoEventProducer;

import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisProducerConfigs;
import com.sysco.speed.architecture.service.schemaregistry.SchemaRegistryDefaultConfigs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import com.sysco.speed.architecture.service.ProducerResult;
import com.sysco.speed.architecture.service.kinesis.KinesisProducerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Kinesis implementation of the Sysco Event Producer
 *
 * Created by chamantha on 7/19/17.
 */

public class SyscoKinesisEventProducer implements SyscoEventProducer {

    private static Logger LOGGER = LoggerFactory.getLogger(SyscoKinesisEventProducer.class);

    private AmazonKinesisClient producer;
    private String streamName;
    private SyscoSchemaRegistryClient schemaClient;

    /**
     * Constructor.
     *
     * @param properties Set of properties.
     */
    public SyscoKinesisEventProducer(Properties properties) throws ConfigurationException {
        LOGGER.info("Creating the Kinesis Event Producer ...");

        KinesisProducerConfigs kinesisProducerConfigs = new KinesisProducerConfigs(properties);
        this.schemaClient = new SyscoSchemaRegistryClient(new SchemaRegistryDefaultConfigs(properties));

        // Set configurations for Kinesis producer client.
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(kinesisProducerConfigs.getMaxConnections());
        config.setConnectionTimeout(kinesisProducerConfigs.getConnectionTimeout());
        config.setSocketTimeout(kinesisProducerConfigs.getSocketTimeout());

        // Provide AWS credentials as static credentials that don't change.
        AWSCredentials awsCredentials = new BasicAWSCredentials(kinesisProducerConfigs.getAccessKey(), kinesisProducerConfigs.getSecretKey());
        AWSCredentialsProvider awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);

        // Create client for accessing Kinesis.
        this.producer = new AmazonKinesisClient(awsCredentialsProvider, config);
        this.streamName = kinesisProducerConfigs.getStreamName();
    }

    /**
     * @param producer Amazon Kinesis client.
     */
    private void setProducer(AmazonKinesisClient producer) {
        this.producer = producer;
    }

    /**
     * @param streamName Name of the stream.
     */
    private void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    /**
     * Submit an event(in binary format) to the Kinesis stream.
     *
     * @param data The data blob to put into the record, which is base64-encoded when the blob is serialized.
     *             When the data blob (the payload before base64-encoding) is added to the partition key size,
     *             the total size must not exceed the maximum record size (1 MB).
     */

    public ProducerResult submitEvent(ByteBuffer data) {

        // Use the current time in milliseconds to generate the partition key.
        long currentTime = System.currentTimeMillis();

        // Create a request to put the data record into the stream.
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setData(data);

        // In the initial stage a randomly generated partition key for the record.
        // This determines which shard in the stream the data record is assigned to.
        putRecordRequest.setPartitionKey(String.format("partitionKey-%d", currentTime));

        // Kinesis producer client adding the record to the stream.
        PutRecordResult putRecordResult = producer.putRecord(putRecordRequest);

        LOGGER.info("Record sent successfully, partition key : " + putRecordRequest.getPartitionKey() +
                " , ShardID : " + putRecordResult.getShardId() +
                " , Sequence number : " + putRecordResult.getSequenceNumber());

        // Return the result for testing
        ProducerResult result = new KinesisProducerResult(putRecordResult.getSequenceNumber(),putRecordResult.getShardId());
        return result;
    }

    /**
     * Submit an event (in json format) to the Kinesis stream.
     *
     * @param json The data blob in json format.
     * @return
     */
    public ProducerResult submitJsonEvent(String json, String schemaVersionId) {
        try{
            // Get Schema
            Schema schema = new Schema.Parser().parse(schemaClient.getSchemaBySchemaVersionId(schemaVersionId));

            // Convert json payload to binary data.
            byte[] bytes = serialize(json, schema);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            // Use the current time in milliseconds to generate the partition key.
            long currentTime = System.currentTimeMillis();

            // Create a request to put the data record into the stream.
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(streamName);
            putRecordRequest.setData(buffer);

            // In the initial stage a randomly generated partition key for the record.
            // This determines which shard in the stream the data record is assigned to.
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", currentTime));

            // Kinesis producer client adding the record to the stream.
            PutRecordResult putRecordResult = producer.putRecord(putRecordRequest);

            LOGGER.info("Record sent successfully, partition key : " + putRecordRequest.getPartitionKey() +
                    " , ShardID : " + putRecordResult.getShardId() +
                    " , Sequence number : " + putRecordResult.getSequenceNumber());

            // Return the result for testing
            ProducerResult result = new KinesisProducerResult(putRecordResult.getSequenceNumber(),putRecordResult.getShardId());
            return result;

        } catch (Exception e) {
            throw new SchemaException("Error in submitting the record.", e);
        }
    }

    /**
     * Convert JSON payload to binary data.
     *
     * @param json
     * @param schema
     * @return byte array
     */
    public static byte[] serialize(String json, Schema schema) {
        try {
            DatumReader<Object> reader = new GenericDatumReader<>(schema);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            return output.toByteArray();
        }catch(IOException e){
            throw new SchemaException("Error in serializing the record.", e);
        }

    }
}
