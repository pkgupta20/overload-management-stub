package com.overload.threadpool.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConfigurationDTO {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationDTO.class);
    private int numberOfMessages;
    private int consumerThreads;
    private int ddrsConsumers;
    private int qcmConsumers;
    private int numberOfIterations;
    private int ddrsExecutorThreadPoolSize;
    private int rmaInputThreadPoolSize;
    private int rmaExecutorThreadPoolSize;
    private int qcmInputAdapterThreadPoolSize;
    private int qcmProcessingTimeMs;
    private int ddrsProcessingTimeMs;
    private int pollTimeInSeconds;
    private int timeOutMs;


    public ConfigurationDTO(Properties properties) {
        try {
            this.numberOfMessages = Integer.parseInt(properties.getProperty("numberOfMessages", "100"));
            this.ddrsConsumers = Integer.parseInt(properties.getProperty("ddrsConsumers", "1"));
            this.consumerThreads = Integer.parseInt(properties.getProperty("consumerThreads", "1"));
            this.qcmConsumers = Integer.parseInt(properties.getProperty("qcmConsumers", "1"));
            this.numberOfIterations = Integer.parseInt(properties.getProperty("numberOfIterations", "1"));
            this.rmaExecutorThreadPoolSize = Integer.parseInt(properties.getProperty("rmaExecutorThreadPoolSize", "10"));
            this.rmaInputThreadPoolSize = Integer.parseInt(properties.getProperty("rmaInputThreadPoolSize", "10"));
            this.qcmInputAdapterThreadPoolSize = Integer.parseInt(properties.getProperty("qcmInputAdapterThreadPoolSize", "10"));
            this.ddrsExecutorThreadPoolSize = Integer.parseInt(properties.getProperty("ddrsExecutorThreadPoolSize", "10"));
            this.qcmProcessingTimeMs = Integer.parseInt(properties.getProperty("qcmProcessingTimeMs", "1"));
            this.ddrsProcessingTimeMs = Integer.parseInt(properties.getProperty("ddrsProcessingTimeMs", "1"));
            this.pollTimeInSeconds = Integer.parseInt(properties.getProperty("pollTimeInSeconds", "60"));
            this.timeOutMs = Integer.parseInt(properties.getProperty("timeOutMs", "100"));
        } catch (Exception e) {
            LOGGER.error("Please check following values " +
                    "in property file numberOfThreads, numberOfMessages, " +
                    "qcmSites, outFile", e);
        }
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public int getConsumerThreads() {
        return consumerThreads;
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

    public int getQcmProcessingTimeMs() {
        return qcmProcessingTimeMs;
    }

    public int getDdrsExecutorThreadPoolSize() {
        return ddrsExecutorThreadPoolSize;
    }

    public int getDdrsProcessingTimeMs() {
        return ddrsProcessingTimeMs;
    }

    public int getPollTimeInSeconds() {
        return pollTimeInSeconds;
    }

    @Override
    public String toString() {
        return "ConfigurationDTO{" +
                "numberOfMessages=" + numberOfMessages +
                ", consumerThreads=" + consumerThreads +
                ", ddrsConsumers=" + ddrsConsumers +
                ", qcmConsumers=" + qcmConsumers +
                ", numberOfIterations=" + numberOfIterations +
                ", ddrsExecutorThreadPoolSize=" + ddrsExecutorThreadPoolSize +
                ", rmaInputThreadPoolSize=" + rmaInputThreadPoolSize +
                ", rmaExecutorThreadPoolSize=" + rmaExecutorThreadPoolSize +
                ", qcmInputAdapterThreadPoolSize=" + qcmInputAdapterThreadPoolSize +
                ", qcmProcessingTimeMs=" + qcmProcessingTimeMs +
                ", ddrsProcessingTimeMs=" + ddrsProcessingTimeMs +
                ", pollTimeInSeconds=" + pollTimeInSeconds +
                ", timeOutMs=" + timeOutMs +
                '}';
    }

    public int getTimeOutMs() {
        return timeOutMs;
    }
}