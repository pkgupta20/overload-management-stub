package com.overload.threadpool;



import com.overload.threadpool.ddrs.DdrsRmaReceiver;
import com.overload.threadpool.ddrs.DdrsRmaSender;
import com.overload.threadpool.ddrs.DdrsStub;
import com.overload.threadpool.qcm.QcmStub;
import com.overload.threadpool.rma.RmaInputThreadPool;
import com.overload.threadpool.util.ConfigFileReader;
import com.overload.threadpool.util.ConfigurationDTO;
import com.overload.threadpool.util.QueueMonitor;
import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class OcsOverloadEmulator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcsOverloadEmulator.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        BlockingQueue<Message> marbenQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Message> marbenResponseQueue = new LinkedBlockingQueue<>();
        List<DdrsRmaSender> ddrsRmaddrsRmaSenders;
        List<DdrsRmaReceiver> ddrsRmaReceivers;
        LOGGER.info("OCS Overload Emulator Started...");


        ConfigFileReader configFileReader = new ConfigFileReader();
        ConfigurationDTO configurationDTO = configFileReader.readPropValues();
        LOGGER.info("{}", configurationDTO);

        int numberOfMessages = configurationDTO.getNumberOfMessages();
        int producerThreads = configurationDTO.getProducerThreads();
        int ddrsConsumers = configurationDTO.getDdrsConsumers();
        int qcmConsumers = configurationDTO.getQcmConsumers();
        int numberOfIterations = configurationDTO.getNumberOfIterations();
        int ddrsExecutorThreadPoolSize = configurationDTO.getDdrsExecutorThreadPoolSize();
        int rmaInputAdapterThreadPoolSize = configurationDTO.getRmaInputThreadPoolSize();
        int rmaExecutorThreadPoolSize = configurationDTO.getRmaExecutorThreadPoolSize();
        int qcmInputAdapterThreadPoolSize = configurationDTO.getQcmInputAdapterThreadPoolSize();
        int qcmProcessingTime = configurationDTO.getQcmProcessingTime();
        int ddrsProcessingTime = configurationDTO.getDdrsProcessingTime();

        List<BlockingQueue<Message>> qcmNodeList = initQueues(qcmConsumers);
        List<BlockingQueue<Message>> qcmResponseQueues = initQueues(ddrsConsumers);
        ddrsRmaddrsRmaSenders = initDdrsRmaSenders(ddrsConsumers, qcmNodeList, rmaInputAdapterThreadPoolSize);
        ddrsRmaReceivers = initDDRSRmaReceivers(qcmResponseQueues, marbenResponseQueue, ddrsExecutorThreadPoolSize);
        NetworkEmulator networkEmulator = new NetworkEmulator(numberOfMessages, numberOfIterations, producerThreads, marbenQueue, marbenResponseQueue);
        DdrsStub ddrsStub = new DdrsStub(marbenQueue, ddrsConsumers, qcmConsumers, ddrsRmaddrsRmaSenders, ddrsRmaReceivers, ddrsProcessingTime);
        QcmStub qcmStub = new QcmStub(qcmNodeList, qcmResponseQueues, qcmInputAdapterThreadPoolSize, rmaExecutorThreadPoolSize, qcmProcessingTime);
        QueueMonitor qMonitor = new QueueMonitor(marbenQueue);
        qMonitor.start();
        networkEmulator.start();
        ddrsStub.start();
        qcmStub.start();
        networkEmulator.stop();
        ddrsStub.stop();
        qMonitor.stop();
        LOGGER.info("MarbenQueue size:{}", marbenQueue.size());
        LOGGER.info("MarbenResponseQueue size:{}", marbenResponseQueue.size());
    }

    private static List<DdrsRmaReceiver> initDDRSRmaReceivers(List<BlockingQueue<Message>> qcmResponseQueues, BlockingQueue<Message> marbenResponseQueue, int ddrsExecutorThreadPoolSize) {
        int ddrsNodes = qcmResponseQueues.size();
        List<DdrsRmaReceiver> ddrsRmaReceivers = new ArrayList<>(ddrsNodes);
        for (BlockingQueue<Message> qcmResponseQueue : qcmResponseQueues) {
            DdrsRmaReceiver ddrsrmaReceiver = new DdrsRmaReceiver(qcmResponseQueue, marbenResponseQueue, ddrsExecutorThreadPoolSize);
            ddrsRmaReceivers.add(ddrsrmaReceiver);
        }
        return ddrsRmaReceivers;
    }

    private static List<DdrsRmaSender> initDdrsRmaSenders(int ddrsConsumers, List<BlockingQueue<Message>> qcmSiteNodeList, int rmaInputAdapterThreadPoolSize) {
        List<DdrsRmaSender> ddrsRmaEmulatorList = new ArrayList<>(ddrsConsumers);
        for (int i = 0; i < ddrsConsumers; i++) {
            DdrsRmaSender ddrsRmaSender = new DdrsRmaSender(getRmaInputThreadPool(qcmSiteNodeList, rmaInputAdapterThreadPoolSize));
            ddrsRmaEmulatorList.add(ddrsRmaSender);

        }
        return ddrsRmaEmulatorList;

    }

    private static RmaInputThreadPool getRmaInputThreadPool(List<BlockingQueue<Message>> qcmSiteNodeList, int rmaInputAdapterThreadPoolSize) {
        ExecutorService service = Executors.newFixedThreadPool(rmaInputAdapterThreadPoolSize);
        return new RmaInputThreadPool(service, qcmSiteNodeList);
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