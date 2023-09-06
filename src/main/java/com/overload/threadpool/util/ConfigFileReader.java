package com.overload.threadpool.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

public final class ConfigFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigFileReader.class);
    public ConfigurationDTO readPropValues() throws IOException {
        InputStream inputStream = null;
        try {
            LOGGER.info("Application configuration file path " + System.getProperty("appConfigFile"));
            Properties props = new Properties();
            String propFileName = "config.properties";
            if (System.getProperty("appConfigFile") != null) {
                inputStream = Files.newInputStream(new File(System.getProperty("appConfigFile")).toPath());
            } else {
                inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            }

            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
            return new ConfigurationDTO(props);
        } catch (Exception e) {
            LOGGER.error("Exception while reading configuration values: ", e);
        } finally {
            if (inputStream != null)
                inputStream.close();
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        ConfigFileReader configFileReader = new ConfigFileReader();

        System.out.println(configFileReader.readPropValues());

    }
}
