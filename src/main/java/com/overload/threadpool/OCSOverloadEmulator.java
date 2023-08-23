package com.overload.threadpool;

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

        int numberOfMessages = 5;
        int producerThreads = 1;
        int ddrsConsumers = 1;
        int qcmConsumers = 1;

        List<BlockingQueue<Message>> qcmNodeList = initQueues(qcmConsumers);
        List<BlockingQueue<Message>> qcmResponseQueues = initQueues(qcmConsumers);
        ddrsRmaEmulators = initDdrsRmaEmulators(ddrsConsumers, qcmNodeList);
        ddrsrmaReceivers = initDDRSRmaReceivers(qcmResponseQueues,marbenResponseQueue,ddrsConsumers);
        WorkloadGenerator workloadGenerator = new WorkloadGenerator(numberOfMessages, producerThreads, marbenQueue, ddrsConsumers,marbenResponseQueue);
        DDRSStub ddrsStub = new DDRSStub(marbenQueue, ddrsConsumers, qcmConsumers, ddrsRmaEmulators,ddrsrmaReceivers);
        QCMStub qcmStub = new QCMStub(qcmNodeList, qcmResponseQueues);
        workloadGenerator.start();
        ddrsStub.start();
        qcmStub.start();
        workloadGenerator.stop();
        ddrsStub.stop();
        LOGGER.info("MarbenQueue size:"+marbenQueue.size());
        LOGGER.info("MarbenResponseQueue size:"+marbenResponseQueue.size());
    }

    private static List<DDRSRMAReceiver> initDDRSRmaReceivers(List<BlockingQueue<Message>> qcmResponseQueues, BlockingQueue<Message> marbenResponseQueue, int producerThreads) {
        List<DDRSRMAReceiver> ddrsrmaReceivers = new ArrayList<>(qcmResponseQueues.size());
        for (int i = 0; i < qcmResponseQueues.size(); i++) {
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
        ExecutorService service = Executors.newFixedThreadPool(1);
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
