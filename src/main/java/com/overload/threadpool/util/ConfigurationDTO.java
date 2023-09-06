package com.overload.threadpool.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConfigurationDTO {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationDTO.class);
    private int numberOfMessages;
    private int producerThreads;
    private int ddrsConsumers;
    private int qcmConsumers;
    private int numberOfIterations;
    private int ddrsExecutorThreadPoolSize;
    private int rmaInputThreadPoolSize;
    private int rmaExecutorThreadPoolSize;
    private int qcmInputAdapterThreadPoolSize;
    private int qcmProcessingTime;
    private int ddrsProcessingTime;


    public ConfigurationDTO(Properties properties) {
        try {
            this.numberOfMessages = Integer.parseInt(properties.getProperty("numberOfMessages", "100"));
            this.ddrsConsumers = Integer.parseInt(properties.getProperty("ddrsConsumers", "1"));
            this.producerThreads = Integer.parseInt(properties.getProperty("producerThreads", "1"));
            this.qcmConsumers = Integer.parseInt(properties.getProperty("qcmConsumers", "1"));
            this.numberOfIterations = Integer.parseInt(properties.getProperty("numberOfIterations", "1"));
            this.rmaExecutorThreadPoolSize = Integer.parseInt(properties.getProperty("rmaExecutorThreadPoolSize", "10"));
            this.rmaInputThreadPoolSize = Integer.parseInt(properties.getProperty("rmaInputThreadPoolSize", "10"));
            this.qcmInputAdapterThreadPoolSize = Integer.parseInt(properties.getProperty("qcmInputAdapterThreadPoolSize", "10"));
            this.ddrsExecutorThreadPoolSize = Integer.parseInt(properties.getProperty("ddrsExecutorThreadPoolSize", "10"));
            this.qcmProcessingTime = Integer.parseInt(properties.getProperty("qcmProcessingTime","1"));
            this.ddrsProcessingTime = Integer.parseInt(properties.getProperty("ddrsProcessingTime","1"));
        } catch (Exception e) {
            LOGGER.error("Please check following values " +
                    "in property file numberOfThreads, numberOfMessages, " +
                    "qcmSites, outFile", e);
        }
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public int getProducerThreads() {
        return producerThreads;
    }

    public int getDdrsConsumers() {
        return ddrsConsumers;
    }

    public int getQcmConsumers() {
        return qcmConsumers;
    }

    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    public int getRmaInputThreadPoolSize() {
        return rmaInputThreadPoolSize;
    }

    public int getRmaExecutorThreadPoolSize() {
        return rmaExecutorThreadPoolSize;
    }

    public int getQcmInputAdapterThreadPoolSize() {
        return qcmInputAdapterThreadPoolSize;
    }

    public int getQcmProcessingTime() {
        return qcmProcessingTime;
    }

    public int getDdrsExecutorThreadPoolSize() {
        return ddrsExecutorThreadPoolSize;
    }

    public int getDdrsProcessingTime() {
        return ddrsProcessingTime;
    }

    @Override
    public String toString() {
        return "ConfigurationDTO{" +
                "numberOfMessages=" + numberOfMessages +
                ", producerThreads=" + producerThreads +
                ", ddrsConsumers=" + ddrsConsumers +
                ", qcmConsumers=" + qcmConsumers +
                ", numberOfIterations=" + numberOfIterations +
                ", ddrsExecutorThreadPoolSize=" + ddrsExecutorThreadPoolSize +
                ", rmaInputThreadPoolSize=" + rmaInputThreadPoolSize +
                ", rmaExecutorThreadPoolSize=" + rmaExecutorThreadPoolSize +
                ", qcmInputAdapterThreadPoolSize=" + qcmInputAdapterThreadPoolSize +
                ", qcmProcessingTime=" + qcmProcessingTime +
                ", ddrsProcessingTime=" + ddrsProcessingTime +
                '}';
    }
}
