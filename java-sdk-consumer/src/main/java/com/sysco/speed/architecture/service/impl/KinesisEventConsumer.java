package com.sysco.speed.architecture.service.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.sysco.speed.architecture.schemaregistry.utill.SyscoSchemaRegistryClient;
import com.sysco.speed.architecture.service.EventConsumer;
import com.sysco.speed.architecture.service.impl.kinesis.configs.KinesisConsumerConfigs;
import com.sysco.speed.architecture.service.impl.kinesis.KinesisEventConsumerRecordProcessor;
import com.sysco.speed.architecture.service.utill.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * This event consumer use Kinesis Client Library (KCL) to process data from Kinesis streams
 * <p>
 * Created by nuwanp on 7/21/17.
 */
public class KinesisEventConsumer implements EventConsumer {

    private static Logger LOGGER = LoggerFactory.getLogger(KinesisEventConsumer.class);
    private KinesisConsumerConfigs kinesisEventConsumerConfig;
    private AWSCredentialsProvider awsCredentialsProvider;
    private SyscoSchemaRegistryClient schemaClient;
    private Observer consumer;

    public KinesisEventConsumer(KinesisConsumerConfigs kinesisEventConsumerConfig, SyscoSchemaRegistryClient schemaClient, Observer observer) {

        this.kinesisEventConsumerConfig = kinesisEventConsumerConfig;
        AWSCredentials awsCredentials = new BasicAWSCredentials(kinesisEventConsumerConfig.getAccessKey(),
                kinesisEventConsumerConfig.getSecretKey());
        awsCredentialsProvider = new StaticCredentialsProvider(awsCredentials);
        this.schemaClient = schemaClient;
        this.consumer = observer;

    }

    /**
     * Process events with schema Id.
     *
     * @param schemaVersionId Schema Id.
     * @return
     */
    public void processEvents(final String schemaVersionId) throws UnknownHostException{

        final String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
                kinesisEventConsumerConfig.getApplicationName(), kinesisEventConsumerConfig.getStreamName(),
                awsCredentialsProvider, workerId);

        if (InitialPositionInStream.AT_TIMESTAMP.equals(kinesisEventConsumerConfig.getInitialPositionInStream())) {
            kinesisClientLibConfiguration.withTimestampAtInitialPositionInStream(kinesisEventConsumerConfig.getTimestamp());
        }else{
            kinesisClientLibConfiguration
                    .withInitialPositionInStream(kinesisEventConsumerConfig.getInitialPositionInStream());
        }

        final IRecordProcessorFactory recordProcessorFactory = new IRecordProcessorFactory() {
            /**
             * {@inheritDoc}
             */
            public IRecordProcessor createProcessor() {
                 return new KinesisEventConsumerRecordProcessor(kinesisEventConsumerConfig, schemaClient, consumer, schemaVersionId);
            }
        };
        Runnable workerRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

                    LOGGER.info("Running %s to process stream %s as worker %s...\n", kinesisEventConsumerConfig.getApplicationName(),
                            kinesisEventConsumerConfig.getStreamName(), workerId);

                    worker.run();
                }catch (Exception e){
                    LOGGER.error("Failed %s to process stream %s as worker %s...\n", kinesisEventConsumerConfig.getApplicationName(),
                            kinesisEventConsumerConfig.getStreamName(), workerId);
                }
            }
        };
        Thread workerThread = new Thread(workerRunnable);
        workerThread.start();
    }

    /**
     * Process events without schema Id.
     *
     * @return
     */
    public void processEvents() throws UnknownHostException{

        final String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
                kinesisEventConsumerConfig.getApplicationName(), kinesisEventConsumerConfig.getStreamName(),
                awsCredentialsProvider, workerId);

        if (InitialPositionInStream.AT_TIMESTAMP.equals(kinesisEventConsumerConfig.getInitialPositionInStream())) {
            kinesisClientLibConfiguration.withTimestampAtInitialPositionInStream(kinesisEventConsumerConfig.getTimestamp());
        }else{
            kinesisClientLibConfiguration
                    .withInitialPositionInStream(kinesisEventConsumerConfig.getInitialPositionInStream());
        }

        final IRecordProcessorFactory recordProcessorFactory = new IRecordProcessorFactory() {
            /**
             * {@inheritDoc}
             */
            public IRecordProcessor createProcessor() {
                 return new KinesisEventConsumerRecordProcessor(kinesisEventConsumerConfig, schemaClient, consumer);
            }
        };
        Runnable workerRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

                    LOGGER.info("Running %s to process stream %s as worker %s...\n", kinesisEventConsumerConfig.getApplicationName(),
                            kinesisEventConsumerConfig.getStreamName(), workerId);

                    worker.run();
                }catch (Exception e){
                    LOGGER.error("Failed %s to process stream %s as worker %s...\n", kinesisEventConsumerConfig.getApplicationName(),
                            kinesisEventConsumerConfig.getStreamName(), workerId);
                }
            }
        };
        Thread workerThread = new Thread(workerRunnable);
        workerThread.start();
    }
}
