package com.overload;

import org.apache.log4j.Logger;

import java.util.Properties;

public class ConfigurationDTO {
    private int numberOfMessages;
    private int numberOfThreads;
    private  int ddrsConsumers;
    private  int rmaInputConsumer;
    private  int rmaOutputQueueConsumers;
    private  int responseQueueConsumers;
    private int numberOfConsumerThreads;
    private int qcmSites;

    private int qcmNodePerSite;
    private String outFile;
    private static final Logger LOGGER = Logger.getLogger(ConfigurationDTO.class);


    public ConfigurationDTO(Properties properties) {
        try {
            this.numberOfThreads = Integer.valueOf(properties.getProperty("numberOfThreads", "10"));
            this.numberOfMessages = Integer.valueOf(properties.getProperty("numberOfMessages", "1000"));
            this.numberOfConsumerThreads = Integer.valueOf(properties.getProperty("numberOfConsumerThreads","10"));
            this.ddrsConsumers = Integer.valueOf(properties.getProperty("ddrsConsumers"));
            this.rmaInputConsumer = Integer.valueOf(properties.getProperty("rmaInputConsumer"));
            this.rmaOutputQueueConsumers = Integer.valueOf(properties.getProperty("rmaOutputQueueConsumers"));
            this.responseQueueConsumers = Integer.valueOf(properties.getProperty("responseQueueConsumers"));
            this.qcmSites = Integer.valueOf(properties.getProperty("qcmSites", "3"));
            this.qcmNodePerSite = Integer.valueOf(properties.getProperty("qcmNodePerSite","3"));
            this.outFile = properties.getProperty("outFile", "target/output");
        }catch(Exception e){
            LOGGER.error("Please check following values " +
                    "in property file numberOfThreads, numberOfMessages, " +
                    "qcmSites, outFile", e);
        }
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public int getDdrsConsumers() {
        return ddrsConsumers;
    }

    public int getRmaInputConsumer() {
        return rmaInputConsumer;
    }

    public int getRmaOutputQueueConsumers() {
        return rmaOutputQueueConsumers;
    }

    public int getResponseQueueConsumers() {
        return responseQueueConsumers;
    }

    public int getNumberOfConsumerThreads() {
        return numberOfConsumerThreads;
    }

    public int getQcmSites() {
        return qcmSites;
    }

    public int getQcmNodePerSite() {
        return qcmNodePerSite;
    }

    public String getOutFile() {
        return outFile;
    }

    @Override
    public String toString() {
        return "ConfigurationDTO{" +
                "numberOfMessages=" + numberOfMessages +
                ", numberOfThreads=" + numberOfThreads +
                ", ddrsCounsumer=" + ddrsConsumers +
                ", rmaInputConsumer=" + rmaInputConsumer +
                ", rmaOutputQueueConsumers=" + rmaOutputQueueConsumers +
                ", responseQueueConsumers=" + responseQueueConsumers +
                ", numberOfConsumerThreads=" + numberOfConsumerThreads +
                ", qcmSites=" + qcmSites +
                ", qcmNodePerSite=" + qcmNodePerSite +
                ", outFile='" + outFile + '\'' +
                '}';
    }
}
