package com.sysco.speed.architecture.service.schemaregistry;
import com.sysco.speed.architecture.service.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by charithap on 9/20/17.
 */
public class SchemaRegistryDefaultConfigs {
    private static Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryDefaultConfigs.class);

    private static String DEFAULT_VALUE = "DEFAULT";

    private String schemaRegistryApiUrl;


    public SchemaRegistryDefaultConfigs(Properties properties) {
        setSchemaRegistryApiUrl(properties.getProperty(EventConstants.SchemaRegistryConstants.SCHEMA_REGISTRY_API_URL, DEFAULT_VALUE));
    }

    public String getSchemaRegistryApiUrl() {
        return schemaRegistryApiUrl;
    }

    public void setSchemaRegistryApiUrl(String schemaRegistryApiUrl) {
        this.schemaRegistryApiUrl = schemaRegistryApiUrl;
    }
}
