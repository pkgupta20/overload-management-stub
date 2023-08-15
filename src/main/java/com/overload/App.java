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
    private static final String FILE_NAME = "target/output.txt";

    private static List<BlockingQueue<Message>> qcmSiteList;

    private static Map<Integer,List<BlockingQueue<Message>>> qcmNodeMap;


    public static void main( String[] args ) throws IOException, InterruptedException {

        int numberOfMessages = 100;
        int numberOfThreads = 10;
        int ddrsCounsumer = 10;
        int rmaInputConsumer = 10;
        int rmaOutputQueueConsumers = 10;
        int responseQueueConsumers = 10;
        int numberOfConsumerThreads = 10;
        int qcmSites = 3;
        int qcmNodePerSite = 3;

        LinkedBlockingQueue<Message> marbenQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> rmaInputQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> rmaOutputQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> responseQueue = new LinkedBlockingQueue<>();
        initQCMSites(qcmSites);
        initQCMNodes(qcmSites, qcmNodePerSite);

        WorkloadGenerator generator = new WorkloadGenerator(numberOfMessages,numberOfThreads,marbenQueue,ddrsCounsumer);

        DDRSStub ddrsStub = new DDRSStub(marbenQueue,numberOfMessages, rmaInputQueue,qcmSites,numberOfThreads,qcmNodePerSite);
        QCMProcessor qcmProcessor = new QCMProcessor(rmaOutputQueue,qcmNodePerSite,qcmNodeMap,rmaOutputQueueConsumers);
        RMAQueueProcessor rmaQueueProcessor = new RMAQueueProcessor(rmaInputQueue,rmaOutputQueue,responseQueue,qcmSiteList,rmaInputConsumer, rmaOutputQueueConsumers, responseQueueConsumers);
        QCMDispatcher qcmDispatcher = new QCMDispatcher(qcmNodeMap,qcmSiteList,qcmNodePerSite);
        ResponseReader responseReader = new ResponseReader(responseQueue,10,FILE_NAME);

        generator.start();
        ddrsStub.start();
        rmaQueueProcessor.sendToQCMSites();
        qcmDispatcher.start();
        qcmProcessor.spawnQCMThread();
        rmaQueueProcessor.sendResponse();
        //responseReader.start();*/
        generator.stop();
        ddrsStub.stop();
        rmaQueueProcessor.stop();
        qcmDispatcher.stop();
        qcmProcessor.stop();
        rmaQueueProcessor.shutdownResponseProcess();
        //responseReader.shutdown();*/
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