package com.overload;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public final class ConfigFileReader {
    private static final Logger LOGGER = Logger.getLogger(ConfigFileReader.class);
    public ConfigurationDTO readPropValues() throws IOException {
        InputStream inputStream = null;
        try {
            LOGGER.info("Application configuration file path " + System.getProperty("appConfigFile"));
            Properties props = new Properties();
            String propFileName = "config.properties";
            if (System.getProperty("appConfigFile") != null) {
                inputStream = new FileInputStream(new File(System.getProperty("appConfigFile")));
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
