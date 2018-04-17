package com.sysco.speed.architecture.service.utill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to implement property file reading and value extraction
 *
 * Created by nuwanp on 7/24/17.
 */
public class PropertyValuesReader {

    private static Logger LOGGER = LoggerFactory.getLogger(PropertyValuesReader.class);

    InputStream inputStream;

    /**
     * Property file in the given location will be processed and values will be returned as a Properties object
     *
     * @param filePath - file location of the properties file
     * @return
     * @throws IOException
     */
    public Properties getPropertyValues(String filePath) throws IOException {
        try {
            Properties prop = new Properties();
            // Reading the property file
            inputStream = new FileInputStream(filePath);
            LOGGER.info("Loading properties from file : " + filePath);
            prop.load(inputStream);
            return prop;
        } finally {
            inputStream.close();
        }
    }
}
