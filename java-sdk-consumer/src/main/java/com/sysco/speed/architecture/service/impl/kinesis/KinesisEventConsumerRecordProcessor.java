package com.sysco.speed.architecture.service.impl.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.sysco.speed.architecture.service.ConsumerResult;
import com.sysco.speed.architecture.exceptions.SchemaException;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisConsumerConfigs;
import com.sysco.speed.architecture.service.constants.EventConstants;
import com.sysco.speed.architecture.service.utill.Observable;
import com.sysco.speed.architecture.service.utill.Observer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.io.EOFException;

/**
 * Processes records and checkpoints progress.
 * <p>
 * Created by nuwanp on 7/24/17.
 */
public class KinesisEventConsumerRecordProcessor implements IRecordProcessor, Observable {

    private static Logger LOGGER = LoggerFactory.getLogger(KinesisEventConsumerRecordProcessor.class);
    private KinesisConsumerConfigs kinesisEventConsumerConfig;
    private SyscoSchemaRegistryClient schemaClient;
    private String kinesisShardId;
    private long nextCheckpointTimeInMillis;
    private String schemaVersionId;
    private String entryType;
    private Observer consumer;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();


    /**
     * Constructor
     *
     * @param kinesisEventConsumerConfig Configurations for Kinesis Event Consumer
     * @param schemaClient Client for invoking Schema Registry
     */
    public KinesisEventConsumerRecordProcessor(KinesisConsumerConfigs kinesisEventConsumerConfig, SyscoSchemaRegistryClient schemaClient, Observer consumer) {
        this.kinesisEventConsumerConfig = kinesisEventConsumerConfig;
        this.schemaClient = schemaClient;
        this.consumer = consumer;
        this.entryType = EventConstants.SchemaRegistryConstants.WITHOUT_SCHEMA;
    }

    /**
     * Constructor
     *
     * @param kinesisEventConsumerConfig Configurations for Kinesis Event Consumer
     * @param schemaClient Client for invoking Schema Registry
     * @param schemaVersionId Schema Id
     */
    public KinesisEventConsumerRecordProcessor(KinesisConsumerConfigs kinesisEventConsumerConfig, SyscoSchemaRegistryClient schemaClient, Observer consumer, String schemaVersionId) {
        this.kinesisEventConsumerConfig = kinesisEventConsumerConfig;
        this.schemaClient = schemaClient;
        this.consumer = consumer;
        this.schemaVersionId = schemaVersionId;
        this.entryType = EventConstants.SchemaRegistryConstants.WITH_SCHEMA;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void initialize(String shardId) {
        LOGGER.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * Notifies the attached consumer class to receive data
     * @param data
     */
    @Override public void notifyObserver(ConsumerResult data) {
        consumer.onDataReceived(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOGGER.info("Processing " + records.size() + " records from " + kinesisShardId);
        // Process records and perform all exception handling.
        if (entryType.equals(EventConstants.SchemaRegistryConstants.WITH_SCHEMA)) {
            processRecordsWithRetries(records, schemaVersionId);
        } else {
            processRecordsWithRetries(records);
        }


        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            //  http://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-implementation-app-java.html(checkpointer);
            nextCheckpointTimeInMillis =
                    System.currentTimeMillis() + kinesisEventConsumerConfig.getCheckpointIntervalMillis();
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOGGER.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /**
     * Process records performing retries as needed with schema. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records, String schemaId) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < kinesisEventConsumerConfig.getRetriesNum(); i++) {
                try {
                    // Logic to process record goes here.
                    processSingleRecord(record, schemaId);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOGGER.warn("Caught throwable while processing record " + record, t);
                }
                // Backoff if we encounter an exception.
                try {
                    Thread.sleep(kinesisEventConsumerConfig.getBackoffTimeInMillis());
                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOGGER.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process records performing retries as needed without schema. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < kinesisEventConsumerConfig.getRetriesNum(); i++) {
                try {
                    // Logic to process record goes here.
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOGGER.warn("Caught throwable while processing record " + record, t);
                }
                // Backoff if we encounter an exception.
                try {
                    Thread.sleep(kinesisEventConsumerConfig.getBackoffTimeInMillis());
                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOGGER.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record with schema.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record, String schemaVersionId) {
        try {
            String data = null;

            // Retrieve binary data
            ByteBuffer buf = record.getData();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            // Retrieve schema from Schema Registry
            Schema schema = new Schema.Parser().parse(schemaClient.getSchemaBySchemaVersionId(schemaVersionId));

            // Deserialize the record
            data = deserialize(bytes, schema);

            //Notifies the observer
            notifyObserver(new ConsumerResult(record.getSequenceNumber(), record.getPartitionKey(),data));

            LOGGER.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", [[[[Data]]]] : " + data);
        }catch(RuntimeException e){
            throw new SchemaException("Error in retrieving the record.", e);
        }
    }

    /**
     * Process a single record without schema.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) {
        String data = null;
        try {
            //Decodes the binary data
            data = decoder.decode(record.getData()).toString();

            //Notifies the observer
            notifyObserver(new ConsumerResult(record.getSequenceNumber(), record.getPartitionKey(),data));
            LOGGER.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", [[[[Data]]]] : " + data);
        } catch (CharacterCodingException e) {
            LOGGER.error("Malformed data: " + data, e);
        }
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        final int retriesNum = kinesisEventConsumerConfig.getRetriesNum();
        LOGGER.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < kinesisEventConsumerConfig.getRetriesNum(); i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOGGER.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (retriesNum - 1)) {
                    LOGGER.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOGGER.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + retriesNum, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOGGER.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
                        e);
                break;
            }
            try {
                Thread.sleep(kinesisEventConsumerConfig.getBackoffTimeInMillis());
            } catch (InterruptedException e) {
                LOGGER.debug("Interrupted sleep", e);
            }
        }
    }

    /**
     * Convert binary byte array to JSON String.
     *
     * @param avro byte array of data
     * @param schema schema for deserialization process
     * @return payload
     */
    public String deserialize(byte[] avro, Schema schema){
        try {
            GenericDatumReader<Object> reader = new GenericDatumReader(schema);
            DatumWriter<Object> writer = new GenericDatumWriter(schema);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            output.flush();
            return new String(output.toByteArray(), "UTF-8");
        }catch(EOFException e){
            try {
                ByteBuffer buf = ByteBuffer.wrap(avro);
                return decoder.decode(buf).toString();
            }catch(CharacterCodingException ex){
                LOGGER.error("Malformed data !!!", ex);
            }
        }catch(IOException e){
            throw new SchemaException("Error in deserializing the record.", e);
        }
        return null;
    }
}
