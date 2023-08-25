package com.overload.threadpool;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class OCSOverloadEmulator {

    private static final Logger LOGGER = Logger.getLogger(OCSOverloadEmulator.class);

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Message> marbenQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Message> marbenResponseQueue = new LinkedBlockingQueue<>();
        List<DDRSRMAEmulator> ddrsRmaEmulators;
        List<DDRSRMAReceiver> ddrsrmaReceivers;

        int numberOfMessages = 1000;
        int producerThreads = 8;
        int ddrsConsumers = 8;
        int qcmConsumers = 8;

        List<BlockingQueue<Message>> qcmNodeList = initQueues(qcmConsumers);
        List<BlockingQueue<Message>> qcmResponseQueues = initQueues(ddrsConsumers);
        ddrsRmaEmulators = initDdrsRmaEmulators(ddrsConsumers, qcmNodeList);
        ddrsrmaReceivers = initDDRSRmaReceivers(qcmResponseQueues,marbenResponseQueue,ddrsConsumers);
        NetworkEmulator networkEmulator = new NetworkEmulator(numberOfMessages, producerThreads, marbenQueue, ddrsConsumers,marbenResponseQueue);
        DDRSStub ddrsStub = new DDRSStub(marbenQueue, ddrsConsumers, qcmConsumers, ddrsRmaEmulators,ddrsrmaReceivers);
        QCMStub qcmStub = new QCMStub(qcmNodeList, qcmResponseQueues);
        QMonitor qMonitor = new QMonitor(marbenQueue);
        qMonitor.start();
        networkEmulator.start();
        ddrsStub.start();
        qcmStub.start();
        networkEmulator.stop();
        ddrsStub.stop();
        qMonitor.stop();
        LOGGER.info("MarbenQueue size:"+marbenQueue.size());
        LOGGER.info("MarbenResponseQueue size:"+marbenResponseQueue.size());
    }

    private static List<DDRSRMAReceiver> initDDRSRmaReceivers(List<BlockingQueue<Message>> qcmResponseQueues, BlockingQueue<Message> marbenResponseQueue, int producerThreads) {
        List<DDRSRMAReceiver> ddrsrmaReceivers = new ArrayList<>(producerThreads);
        for (int i = 0; i < producerThreads; i++) {
            DDRSRMAReceiver ddrsrmaReceiver = new DDRSRMAReceiver(qcmResponseQueues.get(i),marbenResponseQueue,producerThreads);
            ddrsrmaReceivers.add(ddrsrmaReceiver);
        }
        return ddrsrmaReceivers;
    }

    private static List<DDRSRMAEmulator> initDdrsRmaEmulators(int ddrsConsumers, List<BlockingQueue<Message>> qcmSiteNodeList) {
        List<DDRSRMAEmulator> ddrsRmaEmulatorList = new ArrayList<>(ddrsConsumers);
        for (int i = 0; i < ddrsConsumers; i++) {
            DDRSRMAEmulator ddrsrmaEmulator = new DDRSRMAEmulator(getRmaInputThreadPool(qcmSiteNodeList));
            ddrsRmaEmulatorList.add(ddrsrmaEmulator);

        }
        return ddrsRmaEmulatorList;

    }

    private static RMAInputThreadPool getRmaInputThreadPool(List<BlockingQueue<Message>> qcmSiteNodeList) {

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("DDRS-RMA-Executor-Thread-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        ExecutorService service = Executors.newFixedThreadPool(5,factory);
        RMAInputThreadPool rmaInputThreadPool = new RMAInputThreadPool(service, qcmSiteNodeList);
        return rmaInputThreadPool;
    }

    private static List<BlockingQueue<Message>> initQueues(int qcmConsumers) {
        List<BlockingQueue<Message>> qcmSiteNodes = new ArrayList<>();
        for (int i = 0; i < qcmConsumers; i++) {
            BlockingQueue<Message> nodeQueue = new LinkedBlockingQueue<>();
            qcmSiteNodes.add(nodeQueue);
        }
        return qcmSiteNodes;
    }

}
