package com.overload.threadpool.ddrs;


import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DdrsStub {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdrsStub.class);
    private final List<DdrsRmaSender> ddrsRmaEmulators;
    private final List<DdrsRmaReceiver> ddrsRMAReceivers;
    private final BlockingQueue<Message> marbenQueue;
    private final ExecutorService consumerExecutor;

    private final int ddrsConsumers;
    private final int qcmNodes;

    private final int processingTime;



    public DdrsStub(BlockingQueue<Message> marbenQueue, int ddrsConsumers, int qcmNodes, List<DdrsRmaSender> ddrsRmaEmulators, List<DdrsRmaReceiver> ddrsRmaReceivers, int processingTime) {
        this.marbenQueue = marbenQueue;
        this.ddrsConsumers = ddrsConsumers;
        this.qcmNodes = qcmNodes;
        this.ddrsRmaEmulators = ddrsRmaEmulators;
        this.ddrsRMAReceivers = ddrsRmaReceivers;
        this.consumerExecutor = Executors.newFixedThreadPool(this.ddrsConsumers);
        this.processingTime = processingTime;
    }

    public void start() {
        List<Runnable> ddrsNodes = new ArrayList<>(ddrsConsumers);
        for (int i = 0; i < ddrsConsumers; i++) {
            Runnable ddrsNode = new DdrsNode(this.marbenQueue, this.qcmNodes, this.ddrsRmaEmulators.get(i), this.ddrsRMAReceivers.get(i), processingTime);
            ddrsNodes.add(ddrsNode);
        }

        for (Runnable ddrsNode : ddrsNodes) {
            consumerExecutor.execute(ddrsNode);
        }


    }

    public void stop() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("DDRSStub shutdown triggered:");
        consumerExecutor.shutdown();
        long endTime = System.currentTimeMillis();
        LOGGER.info("DDRSStub shutdown passed:{}" , (endTime - startTime));
        boolean terminationResult = consumerExecutor.awaitTermination(10000, TimeUnit.SECONDS);
        LOGGER.info("DDRSStub Terminated normally:{}", terminationResult);

    }


}