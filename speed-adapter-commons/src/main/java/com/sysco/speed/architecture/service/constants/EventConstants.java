package com.sysco.speed.architecture.service.constants;

/**
 * Created by nandunin on 9/28/17.
 */
public class EventConstants {

    // Inner class for constants related to schema registry
    public static class KinesisConstants {
        // Kinesis configuration parameter key names used in property file
        public static String STREAM_NAME = "stream.name";
        public static String ACCESS_KEY = "access.key";
        public static String SECRET_KEY = "secret.key";

        // Producer specific parameters
        public static String MAX_CONNECTIONS = "max.connections";
        public static String CONNECTION_TIME_OUT = "connection.timeout";
        public static String SOCKET_TIME_OUT = "socket.timeout";

        // Consumer specific parameters
        public static String APPLICATION_NAME = "application.name";
        public static String INITIAL_STREAM_POSITION = "initial.stream.position";
        public static String BACKOFF_TIME_IN_MILLIS = "backoff.time.millis";
        public static String NUM_RETRIES = "retries.num";
        public static String CHECKPOINT_INTERVAL_MILLIS = "checkpoint.interval.millis";
        public static String TIMESTAMP = "timestamp";
    }

    // Inner class for constants related to schema registry
    public static class SchemaRegistryConstants {
        public static String SCHEMA_REGISTRY_API_URL = "schema.registry.api.url";
        public static String WITH_SCHEMA = "with.schema";
        public static String WITHOUT_SCHEMA = "without.schema";
    }
}
