package com.overload;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hello world!
 *
 */
public class App
{
    private static final Logger LOGGER = Logger.getLogger(App.class);


    private static List<BlockingQueue<Message>> qcmSiteList;

    private static Map<Integer,List<BlockingQueue<Message>>> qcmNodeMap;


    public static void main( String[] args ) throws IOException, InterruptedException {

        ConfigFileReader configFileReader = new ConfigFileReader();
        ConfigurationDTO configurationDTO = configFileReader.readPropValues();

        String FILE_NAME = "target/output.txt";
//        String FILE_NAME = configurationDTO.getOutFile();
        int numberOfMessages = configurationDTO.getNumberOfMessages();
        int numberOfThreads = configurationDTO.getNumberOfThreads();
        int ddrsConsumers = configurationDTO.getDdrsConsumers();
        int rmaInputConsumer = configurationDTO.getRmaInputConsumer();
        int rmaOutputQueueConsumers = configurationDTO.getRmaOutputQueueConsumers();
        int responseQueueConsumers = configurationDTO.getResponseQueueConsumers();
        int numberOfConsumerThreads = configurationDTO.getNumberOfConsumerThreads();
        int qcmSites = configurationDTO.getQcmSites();
        int qcmNodePerSite = configurationDTO.getQcmNodePerSite();

        LinkedBlockingQueue<Message> marbenQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> rmaInputQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> rmaOutputQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> responseQueue = new LinkedBlockingQueue<>();
        initQCMSites(qcmSites);
        initQCMNodes(qcmSites, qcmNodePerSite);

        WorkloadGenerator generator = new WorkloadGenerator(numberOfMessages,numberOfThreads,marbenQueue,ddrsConsumers);

        DDRSStub ddrsStub = new DDRSStub(marbenQueue,numberOfMessages, rmaInputQueue,qcmSites,numberOfThreads,qcmNodePerSite);
        QCMProcessor qcmProcessor = new QCMProcessor(rmaOutputQueue,qcmNodePerSite,qcmNodeMap,rmaOutputQueueConsumers);
        RMAQueueProcessor rmaQueueProcessor = new RMAQueueProcessor(rmaInputQueue,rmaOutputQueue,responseQueue,qcmSiteList,rmaInputConsumer, rmaOutputQueueConsumers, responseQueueConsumers);
        QCMDispatcher qcmDispatcher = new QCMDispatcher(qcmNodeMap,qcmSiteList,qcmNodePerSite);
        ResponseReader responseReader = new ResponseReader(responseQueue,responseQueueConsumers,FILE_NAME);

        generator.start();
        ddrsStub.start();
        rmaQueueProcessor.sendToQCMSites();
        qcmDispatcher.start();
        qcmProcessor.spawnQCMThread();
        rmaQueueProcessor.sendResponse();
        responseReader.start();
        generator.stop();
        ddrsStub.stop();
        rmaQueueProcessor.stop();
        qcmDispatcher.stop();
        qcmProcessor.stop();
        rmaQueueProcessor.shutdownResponseProcess();
        responseReader.shutdown();
    }

    private static void initQCMSites(int qcmSites) {
        qcmSiteList = new LinkedList<>();
        for (int i = 0; i < qcmSites; i++) {
            qcmSiteList.add(new LinkedBlockingQueue<>());
        }
    }

    private static void initQCMNodes(int qcmSites,int qcmNodes) {
        qcmNodeMap = new HashMap<>();
        for (int i = 0; i < qcmSites; i++) {
            List<BlockingQueue<Message>> listPerSite = new ArrayList<>();
            for (int j = 0; j < qcmNodes; j++) {
                listPerSite.add(new LinkedBlockingQueue<Message>());
            }
            qcmNodeMap.put(i,listPerSite);
        }

    }
}